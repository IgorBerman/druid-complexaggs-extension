package org.apache.druid.query.aggregation.complexaggs.aggregator;

import javax.annotation.Nullable;

import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.BaseObjectColumnValueSelector;

import gnu.trove.map.hash.TLongLongHashMap;

public class LongFrequencyAggregator implements Aggregator {

    private final BaseObjectColumnValueSelector selector;

    private TLongLongHashMap map;

    public LongFrequencyAggregator(BaseObjectColumnValueSelector selector) {
        this.selector = selector;
        this.map = new TLongLongHashMap();
    }

    @Override
    public void aggregate() {
        Object val = selector.getObject();
        TLongLongHashMapUtils.combineWithObject(map, val);
    }

    @Nullable
    @Override
    public Object get() {
        return map;
    }

    @Override
    public float getFloat() {
        throw new UnsupportedOperationException(this.getClass().getSimpleName() + " does not support getFloat()");
    }

    @Override
    public long getLong() {
        throw new UnsupportedOperationException(this.getClass().getSimpleName() + " does not support getLong()");
    }

    @Override
    public double getDouble() {
        throw new UnsupportedOperationException(this.getClass().getSimpleName() + " does not support getDouble()");
    }

    @Override
    public boolean isNull() {
        return false;
    }

    @Override
    public void close() {

    }
}
