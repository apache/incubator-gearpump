package org.apache.calcite.planner;


import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.RelOptTable.ViewExpander;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rules.*;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.util.Util;
import org.apache.calcite.utils.CalciteFrameworkConfiguration;
import org.apache.calcite.validator.CalciteSqlValidator;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

public class CalciteTest {

    private final SqlOperatorTable operatorTable;
    private final FrameworkConfig config;
    private final ImmutableList<RelTraitDef> traitDefs;
    private final SqlParser.Config parserConfig;
    private final SqlRexConvertletTable convertletTable;
    private State state;
    private SchemaPlus defaultSchema;
    private JavaTypeFactory typeFactory;
    private RelOptPlanner planner;
    private RexExecutor executor;
    private RelRoot root;

    public CalciteTest(FrameworkConfig config) {
        this.config = config;
        this.defaultSchema = config.getDefaultSchema();
        this.operatorTable = config.getOperatorTable();
        this.parserConfig = config.getParserConfig();
        this.state = State.STATE_0_CLOSED;
        this.traitDefs = config.getTraitDefs();
        this.convertletTable = config.getConvertletTable();
        this.executor = config.getExecutor();
        reset();
    }

    private void ensure(State state) {
        if (state == this.state) {
            return;
        }
        if (state.ordinal() < this.state.ordinal()) {
            throw new IllegalArgumentException("cannot move to " + state + " from "
                    + this.state);
        }
        state.from(this);
    }

    public void close() {
        typeFactory = null;
        state = State.STATE_0_CLOSED;
    }

    public void reset() {
        ensure(State.STATE_0_CLOSED);
        state = State.STATE_1_RESET;
    }

    private void ready() {
        switch (state) {
            case STATE_0_CLOSED:
                reset();
        }
        ensure(State.STATE_1_RESET);
        Frameworks.withPlanner(
                new Frameworks.PlannerAction<Void>() {
                    public Void apply(RelOptCluster cluster, RelOptSchema relOptSchema,
                                      SchemaPlus rootSchema) {
                        Util.discard(rootSchema); // use our own defaultSchema
                        typeFactory = (JavaTypeFactory) cluster.getTypeFactory();
                        planner = cluster.getPlanner();
                        planner.setExecutor(executor);
                        return null;
                    }
                },
                config);

        state = State.STATE_2_READY;

        // If user specify own traitDef, instead of default default trait,
        // first, clear the default trait def registered with planner
        // then, register the trait def specified in traitDefs.
        if (this.traitDefs != null) {
            planner.clearRelTraitDefs();
            for (RelTraitDef def : this.traitDefs) {
                planner.addRelTraitDef(def);
            }
        }
    }

    private static SchemaPlus rootSchema(SchemaPlus schema) {
        for (; ; ) {
            if (schema.getParentSchema() == null) {
                return schema;
            }
            schema = schema.getParentSchema();
        }
    }

    private CalciteCatalogReader createCatalogReader() {
        SchemaPlus rootSchema = rootSchema(defaultSchema);
        return new CalciteCatalogReader(
                CalciteSchema.from(rootSchema),
                parserConfig.caseSensitive(),
                CalciteSchema.from(defaultSchema).path(null),
                typeFactory);
    }

    private RexBuilder createRexBuilder() {
        return new RexBuilder(typeFactory);
    }

    private SqlConformance conformance() {
        final Context context = config.getContext();
        if (context != null) {
            final CalciteConnectionConfig connectionConfig =
                    context.unwrap(CalciteConnectionConfig.class);
            if (connectionConfig != null) {
                return connectionConfig.conformance();
            }
        }
        return SqlConformanceEnum.DEFAULT;
    }

    /**
     * Implements {@link org.apache.calcite.plan.RelOptTable.ViewExpander}
     * interface for {@link org.apache.calcite.tools.Planner}.
     */
    public class ViewExpanderImpl implements ViewExpander {
        @Override
        public RelRoot expandView(RelDataType rowType, String queryString,
                                  List<String> schemaPath, List<String> viewPath) {
            SqlParser parser = SqlParser.create(queryString, parserConfig);
            SqlNode sqlNode;
            try {
                sqlNode = parser.parseQuery();
            } catch (SqlParseException e) {
                throw new RuntimeException("parse failed", e);
            }

            final SqlConformance conformance = conformance();
            final CalciteCatalogReader catalogReader =
                    createCatalogReader().withSchemaPath(schemaPath);
            final SqlValidator validator =
                    new CalciteSqlValidator(operatorTable, catalogReader, typeFactory,
                            conformance);
            validator.setIdentifierExpansion(true);
            final SqlNode validatedSqlNode = validator.validate(sqlNode);

            final RexBuilder rexBuilder = createRexBuilder();
            final RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
            final SqlToRelConverter.Config config = SqlToRelConverter.configBuilder()
                    .withTrimUnusedFields(false).withConvertTableAccess(false).build();
            final SqlToRelConverter sqlToRelConverter =
                    new SqlToRelConverter(new ViewExpanderImpl(), validator,
                            catalogReader, cluster, convertletTable, config);

            root = sqlToRelConverter.convertQuery(validatedSqlNode, true, false);
            root = root.withRel(sqlToRelConverter.flattenTypes(root.rel, true));
            root = root.withRel(RelDecorrelator.decorrelateQuery(root.rel));

            return CalciteTest.this.root;
        }
    }

    private enum State {
        STATE_0_CLOSED {
            @Override
            void from(CalciteTest planner) {
                planner.close();
            }
        },
        STATE_1_RESET {
            @Override
            void from(CalciteTest planner) {
                planner.ensure(STATE_0_CLOSED);
                planner.reset();
            }
        },
        STATE_2_READY {
            @Override
            void from(CalciteTest planner) {
                STATE_1_RESET.from(planner);
                planner.ready();
            }
        },
        STATE_3_PARSED,
        STATE_4_VALIDATED,
        STATE_5_CONVERTED;

        /**
         * Moves planner's state to this state. This must be a higher state.
         */
        void from(CalciteTest planner) {
            throw new IllegalArgumentException("cannot move from " + planner.state
                    + " to " + this);
        }
    }


    void calTest() throws SqlParseException {

//        String sql = "select t.orders.id from t.orders";
//
//        String sql = "select t.products.id "
//                + "from t.orders, t.products "
//                + "where t.orders.id = t.products.id and quantity>2 ";

        String sql = "SELECT t.products.id AS product_id, t.products.name "
                + "AS product_name, t.orders.id AS order_id "
                + "FROM t.products JOIN t.orders ON t.products.id = t.orders.id  WHERE quantity > 2";

        final SqlParser.Config parserConfig = SqlParser.configBuilder().setLex(Lex.MYSQL).build();

        // Parse the query
        SqlParser parser = SqlParser.create(sql, parserConfig);
        SqlNode sqlNode = parser.parseStmt();

        // Validate the query
        CalciteCatalogReader catalogReader = createCatalogReader();
        SqlValidator validator = SqlValidatorUtil.newValidator(
                SqlStdOperatorTable.instance(), catalogReader, typeFactory, SqlConformance.DEFAULT);
        SqlNode validatedSqlNode = validator.validate(sqlNode);

        // Convert SqlNode to RelNode
        RexBuilder rexBuilder = createRexBuilder();
        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
        SqlToRelConverter sqlToRelConverter = new SqlToRelConverter(
                new ViewExpanderImpl(),
                validator,
                createCatalogReader(),
                cluster,
                convertletTable);
        RelRoot root = sqlToRelConverter.convertQuery(validatedSqlNode, false, true);
        System.out.println(RelOptUtil.toString(root.rel));

        // Optimize the plan
        RelOptPlanner planner = new VolcanoPlanner();

        // Create a set of rules to apply
        Program program = Programs.ofRules(
                FilterProjectTransposeRule.INSTANCE,
                ProjectMergeRule.INSTANCE,
                FilterMergeRule.INSTANCE,
                FilterJoinRule.JOIN,
                LoptOptimizeJoinRule.INSTANCE);

        RelTraitSet traitSet = planner.emptyTraitSet().replace(EnumerableConvention.INSTANCE);

        // Execute the program
        RelNode optimized = program.run(planner, root.rel, traitSet, ImmutableList.<RelOptMaterialization>of(), ImmutableList.<RelOptLattice>of());
        System.out.println(RelOptUtil.toString(optimized));

    }

    public static void main(String[] args) throws ClassNotFoundException, SQLException, SqlParseException {

        Class.forName("org.apache.calcite.jdbc.Driver");
        java.sql.Connection connection = DriverManager.getConnection("jdbc:calcite:");
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        rootSchema.add("t", new ReflectiveSchema(new StreamQueryPlanner.Transactions()));

        FrameworkConfig frameworkConfig = CalciteFrameworkConfiguration.getDefaultconfig(rootSchema);
        CalciteTest ct = new CalciteTest(frameworkConfig);
        ct.ready();
        ct.calTest();

    }
}
