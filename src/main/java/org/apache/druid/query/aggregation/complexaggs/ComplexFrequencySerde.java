package org.apache.druid.query.aggregation.complexaggs;

import java.nio.ByteBuffer;

import javax.annotation.Nullable;

import org.apache.druid.data.input.InputRow;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.complexaggs.aggregator.LongFrequencyAggregatorFactory;
import org.apache.druid.query.aggregation.complexaggs.aggregator.TLongLongHashMapUtils;
import org.apache.druid.segment.GenericColumnSerializer;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.serde.ComplexColumnPartSupplier;
import org.apache.druid.segment.serde.ComplexMetricExtractor;
import org.apache.druid.segment.serde.ComplexMetricSerde;
import org.apache.druid.segment.serde.LargeColumnSupportedComplexColumnSerializer;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import com.google.common.collect.Ordering;

import gnu.trove.map.hash.TLongLongHashMap;

public class ComplexFrequencySerde extends ComplexMetricSerde {

    private static Ordering<TLongLongHashMap> comparator = new Ordering<TLongLongHashMap>() {
        @Override
        public int compare(
                TLongLongHashMap arg1,
                TLongLongHashMap arg2
        ) {
            return LongFrequencyAggregatorFactory.COMPARATOR.compare(arg1, arg2);
        }
    }.nullsFirst();

    @Override
    public String getTypeName() {
        return LongFrequencyAggregatorFactory.TYPE_NAME;
    }

    @Override
    public ComplexMetricExtractor getExtractor() {
        return new ComplexMetricExtractor() {
            @Override
            public Class<TLongLongHashMap> extractedClass() {
                return TLongLongHashMap.class;
            }

            @Nullable
            @Override
            public Object extractValue(InputRow inputRow, String metricName) {
                throw new UnsupportedOperationException("extractValue without an aggregator factory is not supported.");
            }

            @Override
            public TLongLongHashMap extractValue(InputRow inputRow, String metricName, AggregatorFactory agg) {
                Object rawValue = inputRow.getRaw(metricName);

                TLongLongHashMap map = new TLongLongHashMap();
                TLongLongHashMapUtils.combineWithObject(map, rawValue);
                return map;
            }
        };
    }

    @Override
    public void deserializeColumn(ByteBuffer buffer, ColumnBuilder builder) {
        final GenericIndexed column = GenericIndexed.read(buffer, getObjectStrategy(), builder.getFileMapper());
        builder.setComplexColumnSupplier(new ComplexColumnPartSupplier(getTypeName(), column));
    }

    @Override
    public ObjectStrategy getObjectStrategy() {
        return new ObjectStrategy<TLongLongHashMap>() {
            @Override
            public Class<? extends TLongLongHashMap> getClazz() {
                return TLongLongHashMap.class;
            }

            @Override
            public TLongLongHashMap fromByteBuffer(ByteBuffer buffer, int numBytes) {
                buffer.limit(buffer.position() + numBytes);
                return TLongLongHashMapUtils.fromBytes(buffer.array());
            }

            @Override
            public byte[] toBytes(TLongLongHashMap map) {
                return TLongLongHashMapUtils.toBytes(map);
            }

            @Override
            public int compare(TLongLongHashMap o1, TLongLongHashMap o2) {
                return comparator.compare(o1, o2);
            }
        };
    }

    @Override
    public GenericColumnSerializer getSerializer(
            SegmentWriteOutMedium segmentWriteOutMedium,
            String column
    ) {
        return LargeColumnSupportedComplexColumnSerializer.create(segmentWriteOutMedium, column, this.getObjectStrategy());
    }
}
