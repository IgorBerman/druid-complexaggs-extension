package org.apache.druid.query.aggregation.complexaggs.aggregator;

import gnu.trove.map.hash.TLongLongHashMap;
import org.apache.druid.query.aggregation.complexaggs.ComplexAggregatorsExtensionModule;

import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;

public class LongLongFrequencyAggregatorFactoryTest {
    private static final ObjectMapper MAPPER = new DefaultObjectMapper();
    public static final String INPUT_COLUMN = "inputColumn";
    private static final String OUTPUT_COLUMN = "outputColumn";


    static {
        for (Module module : new ComplexAggregatorsExtensionModule().getJacksonModules()) {
            MAPPER.registerModule(module);
        }
    }

    @Test
    public void testSimpleCombine() {

        final LongFrequencyAggregatorFactory agg = new LongFrequencyAggregatorFactory(OUTPUT_COLUMN, INPUT_COLUMN);
        Assert.assertEquals(OUTPUT_COLUMN, agg.getName());
        Assert.assertEquals(INPUT_COLUMN, agg.getFieldName());
        Assert.assertEquals(LongFrequencyAggregatorFactory.TYPE, agg.getIntermediateType());
        Assert.assertEquals(LongFrequencyAggregatorFactory.FINAL_TYPE, agg.getResultType());

        TLongLongHashMap a = new TLongLongHashMap(new long[]{1L, 2L}, new long[]{1L, 2L});
        TLongLongHashMap b = new TLongLongHashMap(new long[]{1L, 3L}, new long[]{1L, 3L});

        TLongLongHashMap expected = new TLongLongHashMap(new long[]{1L, 2L, 3L}, new long[]{2L, 2L, 3L});

        Assert.assertEquals(expected, agg.combine(a, b));

    }

    @Test
    public void testSerde() throws Exception {

        Assert.assertEquals(
                new LongFrequencyAggregatorFactory("billy", "nilly"),
                MAPPER.readValue("{ \"type\" : \"frequency\", \"name\" : \"billy\",  \"fieldName\": \"nilly\"}", LongFrequencyAggregatorFactory.class)
        );

        final LongFrequencyAggregatorFactory agg = new LongFrequencyAggregatorFactory(OUTPUT_COLUMN, INPUT_COLUMN);
        Assert.assertEquals(
                new LongFrequencyAggregatorFactory(OUTPUT_COLUMN, INPUT_COLUMN),
                MAPPER.readValue(MAPPER.writeValueAsBytes(agg), LongFrequencyAggregatorFactory.class)
        );
    }

}