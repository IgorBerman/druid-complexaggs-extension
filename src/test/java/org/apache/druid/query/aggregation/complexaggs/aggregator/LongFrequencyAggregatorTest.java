package org.apache.druid.query.aggregation.complexaggs.aggregator;

import gnu.trove.map.hash.TLongLongHashMap;

import java.nio.ByteBuffer;
import java.util.Comparator;

import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.TestLongColumnSelector;
import org.apache.druid.query.aggregation.TestObjectColumnSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LongFrequencyAggregatorTest {
    private LongFrequencyAggregatorFactory frequencyAggFactory;
    private LongFrequencyAggregatorFactory combiningAggFactory;
    private TestLongColumnSelector valueSelector;
    private TestObjectColumnSelector<Long> objectSelector;
    private ColumnSelectorFactory colSelectorFactory;

    private long[] longs = {1L, 10000000000L, 1L, 10000000000L, 2L};
    private Long[] objects = {1L, 10000000000L, 1L, 10000000000L, 2L};

    @Before
    public void setup() {
        frequencyAggFactory = new LongFrequencyAggregatorFactory("outputField", "inputField", 10);
        combiningAggFactory = (LongFrequencyAggregatorFactory) frequencyAggFactory.getCombiningFactory();
        valueSelector = new TestLongColumnSelector(longs);
        objectSelector = new TestObjectColumnSelector<>(objects);
        colSelectorFactory = EasyMock.createMock(ColumnSelectorFactory.class);
        EasyMock.expect(colSelectorFactory.makeColumnValueSelector("outputField")).andReturn(valueSelector);
        EasyMock.expect(colSelectorFactory.makeColumnValueSelector("inputField")).andReturn(objectSelector);
        EasyMock.replay(colSelectorFactory);
    }

    @Test
    public void testFrequencyAggregator() {
        Aggregator agg = frequencyAggFactory.factorize(colSelectorFactory);

        for (int i = 0; i < longs.length; i++) {
            aggregate(agg);
        }

        TLongLongHashMap result = (TLongLongHashMap) agg.get();

        TLongLongHashMap expected = constructExpectedResult();

        Assert.assertEquals(expected, result);

    }

    private static TLongLongHashMap constructExpectedResult() {
        TLongLongHashMap expected = new TLongLongHashMap();
        expected.put(1L, 2L);
        expected.put(10000000000L, 2L);
        expected.put(2L, 1L);
        return expected;
    }

    @Test
    public void testFrequencyBufferAggregator() {
        BufferAggregator agg = frequencyAggFactory.factorizeBuffered(colSelectorFactory);

        checkBufferedAggregator(agg);
    }

    @Test
    public void testFrequencyBufferAggregatorFromCombiningFactory() {
        BufferAggregator agg = combiningAggFactory.factorizeBuffered(colSelectorFactory);

        checkBufferedAggregator(agg);
    }


    private void checkBufferedAggregator(BufferAggregator agg) {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[frequencyAggFactory.getMaxIntermediateSizeWithNulls()]);
        agg.init(buffer, 0);


        for (int i = 0; i < longs.length; i++) {
            aggregate(agg, buffer, 0);
        }

        TLongLongHashMap expected = constructExpectedResult();
        TLongLongHashMap result = (TLongLongHashMap) agg.get(buffer, 0);

        Assert.assertEquals(expected, result);
    }


    @Test
    public void testCombine() {
        TLongLongHashMap a = new TLongLongHashMap(new long[]{1L, 2L}, new long[]{1L, 2L});
        TLongLongHashMap b = new TLongLongHashMap(new long[]{1L, 3L}, new long[]{1L, 3L});

        TLongLongHashMap expected = new TLongLongHashMap(new long[]{1L, 2L, 3L}, new long[]{2L, 2L, 3L});

        Assert.assertEquals(expected, frequencyAggFactory.combine(a, b));
    }

    @Test
    public void testCombineWithNull() {
        TLongLongHashMap a = new TLongLongHashMap(new long[]{1L, 2L}, new long[]{1L, 2L});
        TLongLongHashMap b = null;

        Assert.assertEquals(a, frequencyAggFactory.combine(a, b));
        Assert.assertEquals(a, frequencyAggFactory.combine(b, a));
        Assert.assertNull(frequencyAggFactory.combine(b, b));
    }


    @Test
    public void testComparator() {
        TLongLongHashMap a = new TLongLongHashMap(new long[]{1L, 2L}, new long[]{1L, 2L});
        TLongLongHashMap b = null;

        Comparator comparator = frequencyAggFactory.getComparator();
        Assert.assertEquals(1, comparator.compare(a, b));
        Assert.assertEquals(0, comparator.compare(a, a));
        Assert.assertEquals(0, comparator.compare(b, b));
        Assert.assertEquals(-1, comparator.compare(b, a));
    }


    private void aggregate(Aggregator agg) {
        agg.aggregate();
        valueSelector.increment();
        objectSelector.increment();
    }

    private void aggregate(BufferAggregator agg, ByteBuffer buff, int position) {
        agg.aggregate(buff, position);
        valueSelector.increment();
        objectSelector.increment();
    }
}