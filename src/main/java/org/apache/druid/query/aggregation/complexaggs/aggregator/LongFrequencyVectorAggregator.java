package org.apache.druid.query.aggregation.complexaggs.aggregator;

import java.nio.ByteBuffer;

import javax.annotation.Nullable;

import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.segment.vector.VectorValueSelector;

import gnu.trove.map.hash.TLongLongHashMap;

public class LongFrequencyVectorAggregator implements VectorAggregator {
    private final VectorValueSelector selector;
    private final LongFrequencyBufferAggregatorHelper innerAggregator;

    public LongFrequencyVectorAggregator(
            VectorValueSelector selector
    ) {
        this.selector = selector;
        this.innerAggregator = new LongFrequencyBufferAggregatorHelper(
        );
    }

    @Override
    public void init(ByteBuffer buf, int position) {
        innerAggregator.init(buf, position);
    }

    @Override
    public void aggregate(ByteBuffer buf, int position, int startRow, int endRow) {
        long[] vector = selector.getLongVector();
        boolean[] isNull = selector.getNullVector();
        TLongLongHashMap map = innerAggregator.get(buf, position);
        for (int i = startRow; i < endRow; i++) {
            if (!isNull(isNull, i)) {
                TLongLongHashMapUtils.combineWithLong(map, vector[i]);
            }
        }
        innerAggregator.put(buf, position, map);
    }

    @Override
    public void aggregate(ByteBuffer buf, int numRows, int[] positions, @Nullable int[] rows, int positionOffset) {
        long[] vector = selector.getLongVector();
        boolean[] isNull = selector.getNullVector();
        for (int i = 0; i < numRows; i++) {
            int position = positions[i] + positionOffset;
            int index = rows != null ? rows[i] : i;
            if (!isNull(isNull, index)) {
                innerAggregator.aggregate(buf, position, vector[index]);
            }
        }
    }

    @Nullable
    @Override
    public Object get(ByteBuffer buf, int position) {
        return innerAggregator.get(buf, position);
    }

    @Override
    public void close() {
        // Nothing to close
    }

    private boolean isNull(@Nullable boolean[] isNull, int index) {
        return (isNull != null && isNull[index]);
    }
}
