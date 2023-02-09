package org.apache.druid.query.aggregation.complexaggs.aggregator;

import static org.apache.druid.query.aggregation.complexaggs.aggregator.LongFrequencyAggregatorFactory.TYPE_NAME;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;

import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.AggregateCombiner;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.ObjectAggregateCombiner;
import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.query.aggregation.complexaggs.ComplexAggregatorsExtensionModule;
import org.apache.druid.query.aggregation.complexaggs.TLongLongHashMapSerializer;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.nested.NestedDataComplexTypeSerde;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.primitives.Ints;
import gnu.trove.map.TLongLongMap;
import gnu.trove.map.hash.TLongLongHashMap;

@JsonTypeName(TYPE_NAME)
public class LongFrequencyAggregatorFactory extends AggregatorFactory {

    public static final int MAX_NUM_OF_DISTINCT_KEYS = 1000;//TODO make it param?
    public static final String TYPE_NAME = "frequency";
    public static final ColumnType TYPE = ColumnType.ofComplex(TYPE_NAME);
    public static final ColumnType FINAL_TYPE = NestedDataComplexTypeSerde.TYPE;

    static final TLongLongHashMapSerializer serializer = new TLongLongHashMapSerializer();

    public static final Comparator COMPARATOR = new Comparator() {
        @Override
        public int compare(Object a, Object b) {
            int aSize = a == null ? 0:((TLongLongMap) a).size();
            int bSize = b == null ? 0:((TLongLongMap) b).size();
            return Ints.compare(aSize, bSize);
        }
    };


    public static final byte DYNAMIC_FREQUENCIES_TYPE_ID = 99; //TODO need to move it to AggregatorUtils
    public static final byte DYNAMIC_FREQUENCIES_POST_AGGREGATOR_ID = 99; //TODO need to move it to PostAggregatorIds

    private final String name;
    private final String fieldName;
    private ObjectMapper jsonMapper = ComplexAggregatorsExtensionModule.OBJECT_MAPPER; //TODO only for POC
//    @Inject TODO object mapper is used to finalize to string until I find the way to finalize it to JSON
//    public void setObjectMapper(@Json final ObjectMapper jsonMapper)
//    {
//        this.jsonMapper = jsonMapper;
//    }

    @JsonCreator
    public LongFrequencyAggregatorFactory(
            @JsonProperty("name") String name,
            @JsonProperty("fieldName") String fieldName
    ) {
        this.name = name;
        this.fieldName = fieldName;
    }

    @Override
    public Aggregator factorize(ColumnSelectorFactory metricFactory) {
        return new LongFrequencyAggregator(
                metricFactory.makeColumnValueSelector(fieldName)
        );
    }

    @Override
    public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory) {
        return new LongFrequencyBufferAggregator(
                metricFactory.makeColumnValueSelector(fieldName)
        );
    }

    @Override
    public VectorAggregator factorizeVector(VectorColumnSelectorFactory columnSelectorFactory) {
        ColumnCapabilities capabilities = columnSelectorFactory.getColumnCapabilities(fieldName);
        if (null == capabilities) {
            throw new IAE("could not find the column type for column %s", fieldName);
        }
        if (capabilities.isNumeric()) {
            return new LongFrequencyVectorAggregator(columnSelectorFactory.makeValueSelector(fieldName)
            );
        } else {
            throw new IAE("cannot vectorize dynamic frequencies aggregation for type %s", capabilities.asTypeString());
        }
    }

    @Override
    public boolean canVectorize(ColumnInspector columnInspector) {
        ColumnCapabilities capabilities = columnInspector.getColumnCapabilities(fieldName);
        return capabilities != null && capabilities.isNumeric();
    }

    @Override
    public Comparator getComparator() {
        return COMPARATOR;
    }

    @Nullable
    @Override
    public Object combine(@Nullable Object lhs, @Nullable Object rhs) {
        if (lhs == null) {
            if (rhs == null) {
                return null;
            } else {
                return rhs;
            }
        } else {
            TLongLongHashMapUtils.combineWithOther((TLongLongHashMap) lhs, (TLongLongHashMap) rhs);
            return lhs;
        }
    }

    @Override
    public AggregateCombiner makeAggregateCombiner() {
        return new ObjectAggregateCombiner() {
            private final TLongLongHashMap combined = new TLongLongHashMap();

            @Override
            public void reset(ColumnValueSelector selector) {
                Object first = selector.getObject();
                TLongLongHashMapUtils.combineWithObject(combined, first);
            }

            @Override
            public void fold(ColumnValueSelector selector) {
                TLongLongHashMap other = (TLongLongHashMap) selector.getObject();
                TLongLongHashMapUtils.combineWithOther(combined, other);
            }

            @Override
            public TLongLongHashMap getObject() {
                return combined;
            }

            @Override
            public Class<TLongLongHashMap> classOfObject() {
                return TLongLongHashMap.class;
            }
        };
    }

    @Override
    public AggregatorFactory getCombiningFactory() {
        return new LongFrequencyAggregatorFactory(
                name,
                name
        );
    }

    @Override
    public AggregatorFactory getMergingFactory(AggregatorFactory other) {
        return new LongFrequencyAggregatorFactory(
                name,
                name
        );
    }

    @Override
    public List<AggregatorFactory> getRequiredColumns() {
        return Collections.singletonList(
                new LongFrequencyAggregatorFactory(
                        fieldName,
                        fieldName
                )
        );
    }

    @Override
    public Object deserialize(Object object) {
        if (object instanceof String) {
            byte[] bytes = StringUtils.decodeBase64(StringUtils.toUtf8((String) object));
            final TLongLongHashMap fbh = TLongLongHashMapUtils.fromBytes(bytes);
            return fbh;
        } else {
            return object;
        }
    }

    @Nullable
    @Override
    public Object finalizeComputation(@Nullable Object object) {
        if (object == null) {
            return null;
        }
        try {
            return jsonMapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @JsonProperty
    @Override
    public String getName() {
        return name;
    }

    @Override
    public List<String> requiredFields() {
        return Collections.singletonList(fieldName);
    }

    /**
     * actual type is {@link TLongLongHashMap}
     */
    @Override
    public ColumnType getIntermediateType() {
        return TYPE;
    }

    /**
     * actual type is {@link TLongLongHashMap} is string //TODO how to convert to JSON?
     */
    @Override
    public ColumnType getResultType() {
        return FINAL_TYPE;
    }

    @Override
    public int getMaxIntermediateSize() {
        return  Long.BYTES * 2 * MAX_NUM_OF_DISTINCT_KEYS; //TODO what can be better here?
    }

    public AggregatorFactory withName(String newName) {
        return new LongFrequencyAggregatorFactory(
                newName,
                getFieldName()
        );
    }

    @Override
    public byte[] getCacheKey() {
        final CacheKeyBuilder builder = new CacheKeyBuilder(DYNAMIC_FREQUENCIES_TYPE_ID)
                .appendString(fieldName);

        return builder.build();
    }

    @JsonProperty
    public String getFieldName() {
        return fieldName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LongFrequencyAggregatorFactory that = (LongFrequencyAggregatorFactory) o;
        return Objects.equals(getName(), that.getName()) &&
                Objects.equals(getFieldName(), that.getFieldName());
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                getName(),
                getFieldName()
        );
    }

    @Override
    public String toString() {
        return "DynamicFrequenciesAggregatorFactory{" +
                "name='" + name + '\'' +
                ", fieldName='" + fieldName + '\'' +
                '}';
    }
}
