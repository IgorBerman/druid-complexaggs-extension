package org.apache.druid.query.aggregation.complexaggs.aggregator;

import java.nio.ByteBuffer;

import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseObjectColumnValueSelector;

public class LongFrequencyBufferAggregator implements BufferAggregator {
    private final BaseObjectColumnValueSelector selector;
    private final LongFrequencyBufferAggregatorHelper innerAggregator;

    public LongFrequencyBufferAggregator(
            BaseObjectColumnValueSelector selector
    ) {
        this.selector = selector;
        this.innerAggregator = new LongFrequencyBufferAggregatorHelper();
    }

    @Override
    public void init(ByteBuffer buf, int position) {
        innerAggregator.init(buf, position);
    }

    @Override
    public void aggregate(ByteBuffer buf, int position) {
        Object val = selector.getObject();
        innerAggregator.aggregate(buf, position, val);
    }

    @Override
    public Object get(ByteBuffer buf, int position) {
        return innerAggregator.get(buf, position);
    }

    @Override
    public float getFloat(ByteBuffer buf, int position) {
        throw new UnsupportedOperationException(this.getClass().getSimpleName() + " does not support getFloat()");
    }

    @Override
    public long getLong(ByteBuffer buf, int position) {
        throw new UnsupportedOperationException(this.getClass().getSimpleName() + " does not support getLong()");
    }

    @Override
    public double getDouble(ByteBuffer buf, int position) {
        throw new UnsupportedOperationException(this.getClass().getSimpleName() + " does not support getDouble()");
    }

    @Override
    public void close() {
        // no resources to cleanup
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector) {
        inspector.visit("selector", selector);
    }
}
