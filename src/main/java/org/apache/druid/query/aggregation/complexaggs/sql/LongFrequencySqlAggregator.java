package org.apache.druid.query.aggregation.complexaggs.sql;

import java.util.List;

import javax.annotation.Nullable;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Optionality;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.complexaggs.aggregator.LongFrequencyAggregatorFactory;
import org.apache.druid.query.aggregation.complexaggs.aggregator.LongFrequencyPostAggregator;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.aggregation.Aggregations;
import org.apache.druid.sql.calcite.aggregation.SqlAggregator;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;

import com.google.common.collect.ImmutableList;

public class LongFrequencySqlAggregator implements SqlAggregator {
    private static final SqlAggFunction FUNCTION_INSTANCE = new DynamicFrequenciesSqlAggFunction();
    private static final String NAME = "FREQUENCY";

    @Override
    public SqlAggFunction calciteFunction() {
        return FUNCTION_INSTANCE;
    }

    @Nullable
    @Override
    public Aggregation toDruidAggregation(
            PlannerContext plannerContext,
            RowSignature rowSignature,
            VirtualColumnRegistry virtualColumnRegistry,
            RexBuilder rexBuilder,
            String name,
            AggregateCall aggregateCall,
            Project project,
            List<Aggregation> existingAggregations,
            boolean finalizeAggregations
    ) {
        final DruidExpression input = Aggregations.toDruidExpressionForNumericAggregator(
                plannerContext,
                rowSignature,
                Expressions.fromFieldAccess(
                        rowSignature,
                        project,
                        aggregateCall.getArgList().get(0)
                )
        );
        if (input == null) {
            return null;
        }

        final RexNode maxNumEntriesOperand = Expressions.fromFieldAccess(
                rowSignature,
                project,
                aggregateCall.getArgList().get(1)
        );

        if (!maxNumEntriesOperand.isA(SqlKind.LITERAL)) {
            // maxNumEntriesOperand must be a literal in order to plan.
            return null;
        }

        final int maxNumEntries = ((Number) RexLiteral.value(maxNumEntriesOperand)).intValue();

        final AggregatorFactory aggregatorFactory;
        final String mapName = aggregateCall.getName();


        // TODO not sure if it's needed...and when...
        for (final Aggregation existing : existingAggregations) {
            for (AggregatorFactory factory : existing.getAggregatorFactories()) {
                if (factory instanceof LongFrequencyAggregatorFactory) {
                    final LongFrequencyAggregatorFactory theFactory = (LongFrequencyAggregatorFactory) factory;

                    // Check input for equivalence.
                    final boolean inputMatches;
                    final DruidExpression virtualInput =
                            virtualColumnRegistry.findVirtualColumnExpressions(theFactory.requiredFields())
                                    .stream()
                                    .findFirst()
                                    .orElse(null);

                    if (virtualInput == null) {
                        inputMatches = input.isDirectColumnAccess()
                                && input.getDirectColumn().equals(theFactory.getFieldName());
                    } else {
                        inputMatches = virtualInput.equals(input);
                    }

                    if (inputMatches && theFactory.getMaxNumberOfEntries() == maxNumEntries) {
                        // Found existing one. Use this.
                        return Aggregation.create(
                                ImmutableList.of(factory),
                                new LongFrequencyPostAggregator(name, mapName)
                        );
                    }
                }
            }
        }

        // No existing match found. Create a new one.
        if (input.isDirectColumnAccess()) {
            aggregatorFactory = new LongFrequencyAggregatorFactory(mapName, input.getDirectColumn(), maxNumEntries);
        } else {
            String virtualColumnName = virtualColumnRegistry.getOrCreateVirtualColumnForExpression(
                    input, chooseCastType(input.getDruidType()));
            aggregatorFactory = new LongFrequencyAggregatorFactory(mapName, virtualColumnName, maxNumEntries);
        }

        return Aggregation.create(aggregatorFactory);
    }

    private ColumnType chooseCastType(ColumnType druidType) {
        if (ColumnType.DOUBLE == druidType || ColumnType.FLOAT == druidType) {
            return ColumnType.LONG;
        } else if (LongFrequencyAggregatorFactory.FINAL_TYPE == druidType || ColumnType.LONG == druidType || LongFrequencyAggregatorFactory.TYPE == druidType) {
            return druidType;
        }
        throw new IAE("Unsupported type " + druidType);
    }

    private static class DynamicFrequenciesSqlAggFunction extends SqlAggFunction {
        private static final ArrayReturnTypeInference RETURN_TYPE_INFERENCE = new ArrayReturnTypeInference();

        private static final String SIGNATURE =
                "'"
                        + NAME
                        + "(column,max_number_of_entries)'\n";

        DynamicFrequenciesSqlAggFunction() {
            super(
                    NAME,
                    null,
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.explicit(SqlTypeName.VARCHAR),
                    null,
                    OperandTypes.or(
                            OperandTypes.and(
                                    OperandTypes.sequence(
                                            SIGNATURE,
                                            OperandTypes.NUMERIC,
                                            OperandTypes.LITERAL
                                    ),
                                    OperandTypes.family(
                                            SqlTypeFamily.NUMERIC,
                                            SqlTypeFamily.NUMERIC
                                    )
                            ),
                            OperandTypes.and(
                                    OperandTypes.sequence(
                                            SIGNATURE,
                                            OperandTypes.STRING,
                                            OperandTypes.LITERAL
                                    ),
                                    OperandTypes.family(
                                            SqlTypeFamily.STRING,
                                            SqlTypeFamily.NUMERIC
                                    )
                            ),
                            OperandTypes.and(
                                    OperandTypes.sequence(
                                            SIGNATURE,
                                            OperandTypes.ANY,
                                            OperandTypes.NUMERIC
                                    ),
                                    OperandTypes.family(
                                            SqlTypeFamily.ANY,
                                            SqlTypeFamily.NUMERIC)
                            )
                    ),
                    SqlFunctionCategory.USER_DEFINED_FUNCTION,
                    false,
                    false,
                    Optionality.FORBIDDEN
            );
        }
    }

    static class ArrayReturnTypeInference implements SqlReturnTypeInference {
        ArrayReturnTypeInference() {
        }

        public RelDataType inferReturnType(SqlOperatorBinding sqlOperatorBinding) {
            RelDataType type = sqlOperatorBinding.getTypeFactory().createSqlType(SqlTypeName.BINARY);
            return sqlOperatorBinding.getTypeFactory().createArrayType(type, -1L);
        }
    }

}
