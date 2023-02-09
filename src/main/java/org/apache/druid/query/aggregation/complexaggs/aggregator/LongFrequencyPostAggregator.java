package org.apache.druid.query.aggregation.complexaggs.aggregator;

import static org.apache.druid.query.aggregation.complexaggs.aggregator.LongFrequencyAggregatorFactory.TYPE_NAME;

import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.column.ColumnType;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Sets;

import gnu.trove.map.hash.TLongLongHashMap;

@JsonTypeName(TYPE_NAME)
public class LongFrequencyPostAggregator implements PostAggregator {
    static final Comparator COMPARATOR = LongFrequencyAggregatorFactory.COMPARATOR;
    private final String name;
    private final String fieldName;

    @JsonCreator
    public LongFrequencyPostAggregator(
            @JsonProperty("name") String name,
            @JsonProperty("fieldName") String fieldName
    ) {
        this.name = name;
        this.fieldName = fieldName;
    }

    @Override
    public Comparator getComparator() {
        return COMPARATOR;
    }

    @Override
    public Set<String> getDependentFields() {
        return Sets.newHashSet(fieldName);
    }

    @Override
    public Object compute(Map<String, Object> values) {
        Object value = values.get(fieldName);
        TLongLongHashMap map = new TLongLongHashMap();
        TLongLongHashMapUtils.combineWithObject(map, value);
        return map;
    }

    @Nullable
    @Override
    public String getName() {
        return name;
    }

    @Override
    public ColumnType getType(ColumnInspector signature) {
        return LongFrequencyAggregatorFactory.FINAL_TYPE;
    }

    @Override
    public PostAggregator decorate(Map<String, AggregatorFactory> aggregators) {
        return this;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "{" +
                "name='" + name + '\'' +
                ", fieldName='" + fieldName + '\'' +
                '}';
    }

    @Override
    public byte[] getCacheKey() {
        return new CacheKeyBuilder(LongFrequencyAggregatorFactory.DYNAMIC_FREQUENCIES_POST_AGGREGATOR_ID)
                .appendString(fieldName)
                .build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LongFrequencyPostAggregator that = (LongFrequencyPostAggregator) o;
        return Objects.equals(name, that.name) && Objects.equals(fieldName, that.fieldName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, fieldName);
    }
}
