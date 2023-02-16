package org.apache.druid.query.aggregation.complexaggs.aggregator.sql;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.FloatDimensionSchema;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimeAndDimsParseSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.aggregation.complexaggs.ComplexAggregatorsExtensionModule;
import org.apache.druid.query.aggregation.complexaggs.aggregator.LongFrequencyAggregatorFactory;
import org.apache.druid.query.aggregation.complexaggs.aggregator.TLongLongHashMapUtils;
import org.apache.druid.query.aggregation.complexaggs.sql.LongFrequencySqlAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;
import org.apache.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import gnu.trove.map.hash.TLongLongHashMap;

public class LongFrequencySqlAggregatorTest extends BaseCalciteQueryTest {
    private static final DruidOperatorTable OPERATOR_TABLE = new DruidOperatorTable(
            ImmutableSet.of(new LongFrequencySqlAggregator()),
            ImmutableSet.of()
    );

    @Override
    public Iterable<? extends Module> getJacksonModules() {
        return Iterables.concat(super.getJacksonModules(),
                ImmutableSet.of(ComplexAggregatorsExtensionModule.createSubTypesModule(), ComplexAggregatorsExtensionModule.createTestSerdeModule()));
    }

    private static final String DATASOURCE = "freq_ds";
    private static final String TIMESTAMP_COLUMN = "t";
    private static final InputRowParser<Map<String, Object>> PARSER_NUMERIC_DIMS;

    private final static ImmutableList<DimensionSchema> DIMENSION_SCHEMAS = ImmutableList.<DimensionSchema>builder()
                .addAll(DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim1")))
                .add(new DoubleDimensionSchema("d1"))
                .add(new FloatDimensionSchema("f1"))
                .add(new LongDimensionSchema("l1"))
                .add(new StringDimensionSchema("freq_str"))
                .build();
    static {

        PARSER_NUMERIC_DIMS = new MapInputRowParser(
                new TimeAndDimsParseSpec(
                        new TimestampSpec(TIMESTAMP_COLUMN, "iso", null),
                        new DimensionsSpec(
                                DIMENSION_SCHEMAS
                        )
                )
        );
    }

    public static final List<ImmutableMap<String, Object>> RAW_ROWS_WITH_REPEATING_NUMERICS = ImmutableList.of(
            ImmutableMap.<String, Object>builder()
                    .put("t", "2000-01-01")
                    .put("dim1", "a")
                    .put("d1", 7.0)
                    .put("f1", 7.0f)
                    .put("l1", 7L)
                    .put("freq_str", "[0,1]")
                    .build(),
            ImmutableMap.<String, Object>builder()
                    .put("t", "2000-01-02")
                    .put("dim1", "b")
                    .put("d1", 7.0)
                    .put("f1", 7.0f)
                    .put("l1", 7L)
                    .put("freq_str", "[0,1]")
                    .build(),
            ImmutableMap.<String, Object>builder()
                    .put("t", "2000-01-03")
                    .put("dim1", "c")
                    .put("d1", 8.0)
                    .put("f1", 8.0f)
                    .put("l1", 8L)
                    .put("freq_str", "[0,1]")
                    .build(),
            ImmutableMap.<String, Object>builder()
                    .put("t", "2001-01-01")
                    .put("dim1", "c")
                    .put("d1", 8.0)
                    .put("f1", 8.0f)
                    .put("l1", 8L)
                    .put("freq_str", "[1,2]")
                    .build(),
            ImmutableMap.<String, Object>builder()
                    .put("t", "2001-01-02")
                    .put("dim1", "c")
                    .put("d1", 8.0)
                    .put("f1", 8.0f)
                    .put("l1", 8L)
                    .put("freq_str", "[1,2]")
                    .build(),
            ImmutableMap.<String, Object>builder()
                    .put("t", "2001-01-03")
                    .put("dim1", "c")
                    .build()
    );

    public static InputRow createRow(final ImmutableMap<String, ?> map, InputRowParser<Map<String, Object>> parser) {
        return parser.parseBatch((Map<String, Object>) map).get(0);
    }


    public static final List<InputRow> ROWS_WITH_REPEATING_NUMERICS =
            RAW_ROWS_WITH_REPEATING_NUMERICS.stream().map(raw -> createRow(raw, PARSER_NUMERIC_DIMS)).collect(Collectors.toList());

    @Override
    public SpecificSegmentsQuerySegmentWalker createQuerySegmentWalker() throws IOException {
        final QueryableIndex index =
                IndexBuilder.create()
                        .tmpDir(temporaryFolder.newFolder())
                        .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                        .schema(
                                new IncrementalIndexSchema.Builder()
                                        .withDimensionsSpec(
                                                new DimensionsSpec(
                                                        DIMENSION_SCHEMAS
                                                )
                                        )
                                        .withRollup(false)
                                        .build()
                        )
                        .rows(ROWS_WITH_REPEATING_NUMERICS)
                        .buildMMappedIndex();

        return new SpecificSegmentsQuerySegmentWalker(conglomerate).add(
                DataSegment.builder()
                        .dataSource(DATASOURCE)
                        .interval(index.getDataInterval())
                        .version("1")
                        .shardSpec(new LinearShardSpec(0))
                        .size(0)
                        .build(),
                index
        );
    }

    @Override
    public DruidOperatorTable createOperatorTable() {
        return OPERATOR_TABLE;
    }


    @Test
    public void testFrequencyOverLongColumn() throws JsonProcessingException {

        TLongLongHashMap expectedMap = new TLongLongHashMap(new long[]{7, 8, 0}, new long[]{2, 3, 1});
        final List<Object[]> expectedResults = ImmutableList.of(
                new Object[]{
                        TLongLongHashMapUtils.toStringSerializedForm(expectedMap)
                }
        );

        testQuery(
                "SELECT \n"
                        + "FREQUENCY(l1, 100) as l1_freq \n"
                        + "FROM " + DATASOURCE,
                ImmutableList.of(
                        Druids.newTimeseriesQueryBuilder()
                                .dataSource(DATASOURCE)
                                .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                                .granularity(Granularities.ALL)
                                .aggregators(ImmutableList.of(
                                        new LongFrequencyAggregatorFactory("l1_freq", "l1", 100)
                                ))
                                .context(QUERY_CONTEXT_DEFAULT)
                                .build()
                ),
                expectedResults
        );
    }


    @Test
    public void testFrequencyOverDoubleColumn() throws JsonProcessingException {
        TLongLongHashMap expectedMap = new TLongLongHashMap(new long[]{7, 8, 0}, new long[]{2, 3, 1});
        final List<Object[]> expectedResults = ImmutableList.of(
                new Object[]{
                        TLongLongHashMapUtils.toStringSerializedForm(expectedMap),
                }
        );

        testQuery(
                "SELECT \n"
                        + "FREQUENCY(d1, 100) as d1_freq \n"
                        + "FROM " + DATASOURCE,
                ImmutableList.of(
                        Druids.newTimeseriesQueryBuilder()
                                .dataSource(DATASOURCE)
                                .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                                .granularity(Granularities.ALL)
                                //.virtualColumns(expressionVirtualColumn("v0", "CAST(\"d1\", 'LONG')", ColumnType.LONG))
                                .aggregators(ImmutableList.of(
                                        new LongFrequencyAggregatorFactory("d1_freq", "d1", 100)
                                ))
                                .context(QUERY_CONTEXT_DEFAULT)
                                .build()
                ),
                expectedResults
        );
    }


    @Test
    public void testFrequencyOverStringColumn() throws JsonProcessingException {
        cannotVectorize();//TODO what is this??
        TLongLongHashMap expectedMap = new TLongLongHashMap(new long[]{0,1}, new long[]{3,4}); //every string was translated to 0 and we have 6 rows...
        final List<Object[]> expectedResults = ImmutableList.of(
                new Object[]{
                        TLongLongHashMapUtils.toStringSerializedForm(expectedMap),
                }
        );

        testQuery(
                "SELECT \n"
                        + "FREQUENCY(freq_str, 100) as agg_freq \n"
                        + "FROM " + DATASOURCE,
                ImmutableList.of(
                        Druids.newTimeseriesQueryBuilder()
                                .dataSource(DATASOURCE)
                                .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                                .granularity(Granularities.ALL)
                                .virtualColumns(expressionVirtualColumn("v0", "\"freq_str\"", ColumnType.STRING))
                                .aggregators(ImmutableList.of(
                                        new LongFrequencyAggregatorFactory("agg_freq", "v0", 100)
                                ))
                                .context(QUERY_CONTEXT_DEFAULT)
                                .build()
                ),
                expectedResults
        );
    }

    @Test
    public void testGroupByAggregator() throws JsonProcessingException {
        TLongLongHashMap aExpectedMap = new TLongLongHashMap(new long[]{7}, new long[]{1});
        TLongLongHashMap bExpectedMap = new TLongLongHashMap(new long[]{7}, new long[]{1});
        TLongLongHashMap cExpectedMap = new TLongLongHashMap(new long[]{0,8}, new long[]{1,3});
        final List<Object[]> expectedResults = ImmutableList.of(
                new Object[]{
                        "a", TLongLongHashMapUtils.toStringSerializedForm(aExpectedMap)
                },
                new Object[] {
                    "b", TLongLongHashMapUtils.toStringSerializedForm(bExpectedMap)

                },
                new Object[] {
                        "c", TLongLongHashMapUtils.toStringSerializedForm(cExpectedMap)

                }
        );

        RowSignature resultRowSignature = RowSignature.builder()
                .add("dim1", ColumnType.STRING)
                .add("l1_freq", ColumnType.LONG_ARRAY).build();
        testQuery(
                "SELECT \n"
                        + "dim1, \n"
                        + "FREQUENCY(l1, 100) as l1_freq \n"
                        + "FROM " + DATASOURCE + " "
                        + "GROUP BY dim1 ",
                ImmutableList.of(
                        GroupByQuery.builder()
                                .setDataSource(DATASOURCE)
                                .setInterval(querySegmentSpec(Filtration.eternity()))
                                .setGranularity(Granularities.ALL)
                                .setDimensions(new DefaultDimensionSpec("dim1", "_d0", ColumnType.STRING))
                                .setAggregatorSpecs(
                                        aggregators(
                                                new LongFrequencyAggregatorFactory("l1_freq", "l1", 100)
                                        )
                                )
                                .setContext(QUERY_CONTEXT_DEFAULT)
                                .build()
                ),
                expectedResults
        );


    }
//
//  @Test
//  public void testFrequencyOnComplexColumn()
//  {
//    cannotVectorize();
//
//    final List<Object[]> expectedResults = ImmutableList.of(
//        new Object[]{
//            1.0299999713897705,
//            3.5,
//            6.293333530426025,
//            6.470000267028809,
//            6.494999885559082,
//            5.497499942779541,
//            6.499499797821045
//        }
//    );
//
//    testQuery(
//        "SELECT\n"
//        + "APPROX_QUANTILE_FIXED_BUCKETS(fbhist_m1, 0.01, 20, 0.0, 10.0),\n"
//        + "APPROX_QUANTILE_FIXED_BUCKETS(fbhist_m1, 0.5, 20, 0.0, 10.0),\n"
//        + "APPROX_QUANTILE_FIXED_BUCKETS(fbhist_m1, 0.98, 30, 0.0, 10.0),\n"
//        + "APPROX_QUANTILE_FIXED_BUCKETS(fbhist_m1, 0.99, 20, 0.0, 10.0),\n"
//        + "APPROX_QUANTILE_FIXED_BUCKETS(fbhist_m1, 0.99, 20, 0.0, 10.0) FILTER(WHERE dim1 = 'abc'),\n"
//        + "APPROX_QUANTILE_FIXED_BUCKETS(fbhist_m1, 0.999, 20, 0.0, 10.0) FILTER(WHERE dim1 <> 'abc'),\n"
//        + "APPROX_QUANTILE_FIXED_BUCKETS(fbhist_m1, 0.999, 20, 0.0, 10.0) FILTER(WHERE dim1 = 'abc')\n"
//        + "FROM foo",
//        ImmutableList.of(
//            Druids.newTimeseriesQueryBuilder()
//                  .dataSource(CalciteTests.DATASOURCE1)
//                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
//                  .granularity(Granularities.ALL)
//                  .aggregators(ImmutableList.of(
//                      new FixedBucketsHistogramAggregatorFactory(
//                          "a0:agg",
//                          "fbhist_m1",
//                          20,
//                          0.0,
//                          10.0,
//                          FixedBucketsHistogram.OutlierHandlingMode.IGNORE,
//                          false
//                      ),
//                      new FixedBucketsHistogramAggregatorFactory(
//                          "a2:agg",
//                          "fbhist_m1",
//                          30,
//                          0.0,
//                          10.0,
//                          FixedBucketsHistogram.OutlierHandlingMode.IGNORE,
//                          false
//                      ),
//                      new FilteredAggregatorFactory(
//                          new FixedBucketsHistogramAggregatorFactory(
//                              "a4:agg",
//                              "fbhist_m1",
//                              20,
//                              0.0,
//                              10.0,
//                              FixedBucketsHistogram.OutlierHandlingMode.IGNORE,
//                              false
//                          ),
//                          new SelectorDimFilter("dim1", "abc", null)
//                      ),
//                      new FilteredAggregatorFactory(
//                          new FixedBucketsHistogramAggregatorFactory(
//                              "a5:agg",
//                              "fbhist_m1",
//                              20,
//                              0.0,
//                              10.0,
//                              FixedBucketsHistogram.OutlierHandlingMode.IGNORE,
//                              false
//                          ),
//                          new NotDimFilter(new SelectorDimFilter("dim1", "abc", null))
//                      )
//                  ))
//                  .postAggregators(
//                      new QuantilePostAggregator("a0", "a0:agg", 0.01f),
//                      new QuantilePostAggregator("a1", "a0:agg", 0.50f),
//                      new QuantilePostAggregator("a2", "a2:agg", 0.98f),
//                      new QuantilePostAggregator("a3", "a0:agg", 0.99f),
//                      new QuantilePostAggregator("a4", "a4:agg", 0.99f),
//                      new QuantilePostAggregator("a5", "a5:agg", 0.999f),
//                      new QuantilePostAggregator("a6", "a4:agg", 0.999f)
//                  )
//                  .context(QUERY_CONTEXT_DEFAULT)
//                  .build()
//        ),
//        expectedResults
//    );
//  }
//
//  @Test
//  public void testQuantileOnInnerQuery()
//  {
//    final List<Object[]> expectedResults;
//    if (NullHandling.replaceWithDefault()) {
//      expectedResults = ImmutableList.of(new Object[]{7.0, 11.940000534057617});
//    } else {
//      expectedResults = ImmutableList.of(new Object[]{5.25, 8.920000076293945});
//    }
//
//    testQuery(
//        "SELECT AVG(x), APPROX_QUANTILE_FIXED_BUCKETS(x, 0.98, 100, 0.0, 100.0)\n"
//        + "FROM (SELECT dim2, SUM(m1) AS x FROM foo GROUP BY dim2)",
//        ImmutableList.of(
//            GroupByQuery.builder()
//                        .setDataSource(
//                            new QueryDataSource(
//                                GroupByQuery.builder()
//                                            .setDataSource(CalciteTests.DATASOURCE1)
//                                            .setInterval(new MultipleIntervalSegmentSpec(ImmutableList.of(
//                                                Filtration.eternity())))
//                                            .setGranularity(Granularities.ALL)
//                                            .setDimensions(new DefaultDimensionSpec("dim2", "d0"))
//                                            .setAggregatorSpecs(
//                                                ImmutableList.of(
//                                                    new DoubleSumAggregatorFactory("a0", "m1")
//                                                )
//                                            )
//                                            .setContext(QUERY_CONTEXT_DEFAULT)
//                                            .build()
//                            )
//                        )
//                        .setInterval(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
//                        .setGranularity(Granularities.ALL)
//                        .setAggregatorSpecs(
//                            new DoubleSumAggregatorFactory("_a0:sum", "a0"),
//                            new CountAggregatorFactory("_a0:count"),
//                            new FixedBucketsHistogramAggregatorFactory(
//                                "_a1:agg",
//                                "a0",
//                                100,
//                                0,
//                                100.0d,
//                                FixedBucketsHistogram.OutlierHandlingMode.IGNORE,
//                                false
//                            )
//                        )
//                        .setPostAggregatorSpecs(
//                            ImmutableList.of(
//                                new ArithmeticPostAggregator(
//                                    "_a0",
//                                    "quotient",
//                                    ImmutableList.of(
//                                        new FieldAccessPostAggregator(null, "_a0:sum"),
//                                        new FieldAccessPostAggregator(null, "_a0:count")
//                                    )
//                                ),
//                                new QuantilePostAggregator("_a1", "_a1:agg", 0.98f)
//                            )
//                        )
//                        .setContext(QUERY_CONTEXT_DEFAULT)
//                        .build()
//        ),
//        expectedResults
//    );
//  }
//
//  @Test
//  public void testEmptyTimeseriesResults()
//  {
//    cannotVectorize();
//
//    testQuery(
//        "SELECT\n"
//        + "APPROX_QUANTILE_FIXED_BUCKETS(fbhist_m1, 0.01, 20, 0.0, 10.0),\n"
//        + "APPROX_QUANTILE_FIXED_BUCKETS(m1, 0.01, 20, 0.0, 10.0)\n"
//        + "FROM foo WHERE dim2 = 0",
//        ImmutableList.of(
//            Druids.newTimeseriesQueryBuilder()
//                  .dataSource(CalciteTests.DATASOURCE1)
//                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
//                  .granularity(Granularities.ALL)
//                  .filters(bound("dim2", "0", "0", false, false, null, StringComparators.NUMERIC))
//                  .aggregators(ImmutableList.of(
//                      new FixedBucketsHistogramAggregatorFactory(
//                          "a0:agg",
//                          "fbhist_m1",
//                          20,
//                          0.0,
//                          10.0,
//                          FixedBucketsHistogram.OutlierHandlingMode.IGNORE,
//                          false
//                      ),
//                      new FixedBucketsHistogramAggregatorFactory(
//                          "a1:agg",
//                          "m1",
//                          20,
//                          0.0,
//                          10.0,
//                          FixedBucketsHistogram.OutlierHandlingMode.IGNORE,
//                          false
//                      )
//
//                  ))
//                  .postAggregators(
//                      new QuantilePostAggregator("a0", "a0:agg", 0.01f),
//                      new QuantilePostAggregator("a1", "a1:agg", 0.01f)
//                  )
//                  .context(QUERY_CONTEXT_DEFAULT)
//                  .build()
//        ),
//        ImmutableList.of(
//            new Object[]{0.0, 0.0}
//        )
//    );
//  }
//
//
}
