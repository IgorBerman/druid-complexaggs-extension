package org.apache.druid.query.aggregation.complexaggs.aggregator;

import java.nio.ByteBuffer;

import javax.annotation.Nullable;

import gnu.trove.map.hash.TLongLongHashMap;

/**
 * A helper class used by {@link LongFrequencyBufferAggregator} and
 * {@link LongFrequencyVectorAggregator} for aggregation operations on byte buffers.
 * Getting the object from value selectors is outside this class.
 */
final class LongFrequencyBufferAggregatorHelper {

    public void init(ByteBuffer buf, int position) {
        ByteBuffer mutationBuffer = buf.duplicate();
        mutationBuffer.position(position);
        TLongLongHashMap map = new TLongLongHashMap();
        mutationBuffer.put(TLongLongHashMapUtils.toBytes(map));
    }

    public void aggregate(ByteBuffer buf, int position, @Nullable Object val) {
        ByteBuffer mutationBuffer = buf.duplicate();
        mutationBuffer.position(position);

        TLongLongHashMap h0 = TLongLongHashMapUtils.fromBuffer(mutationBuffer);
        TLongLongHashMapUtils.combineWithObject(h0, val);

        mutationBuffer.position(position);
        mutationBuffer.put(TLongLongHashMapUtils.toBytes(h0));
    }

    public TLongLongHashMap get(ByteBuffer buf, int position) {
        ByteBuffer mutationBuffer = buf.duplicate();
        mutationBuffer.position(position);
        return TLongLongHashMapUtils.fromBuffer(mutationBuffer);
    }

    public void put(ByteBuffer buf, int position, TLongLongHashMap map) {
        ByteBuffer mutationBuffer = buf.duplicate();
        mutationBuffer.position(position);
        mutationBuffer.put(TLongLongHashMapUtils.toBytes(map));
    }
}
