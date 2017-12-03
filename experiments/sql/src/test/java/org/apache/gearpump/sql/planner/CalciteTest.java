/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gearpump.sql.planner;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
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
import org.apache.calcite.rel.rules.LoptOptimizeJoinRule;
import org.apache.calcite.rel.rules.SortRemoveRule;
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
import org.apache.calcite.tools.*;
import org.apache.calcite.util.Util;
import org.apache.gearpump.sql.rule.GearFilterRule;
import org.apache.gearpump.sql.table.SampleTransactions;
import org.apache.gearpump.sql.utils.CalciteFrameworkConfiguration;
import org.apache.gearpump.sql.validator.CalciteSqlValidator;
import org.junit.Before;
import org.junit.Test;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class CalciteTest {

  private SqlOperatorTable operatorTable;
  private FrameworkConfig config;
  private ImmutableList<RelTraitDef> traitDefs;
  private SqlParser.Config parserConfig;
  private SqlRexConvertletTable convertletTable;
  private State state;
  private SchemaPlus defaultSchema;
  private JavaTypeFactory typeFactory;
  private RelOptPlanner planner;
  private RexExecutor executor;
  private RelRoot root;

  @Before
  public void setFrameworkConfig() throws ClassNotFoundException, SQLException {

    Class.forName("org.apache.calcite.jdbc.Driver");
    java.sql.Connection connection = DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    rootSchema.add("t", new ReflectiveSchema(new StreamQueryPlanner.Transactions()));

    config = CalciteFrameworkConfiguration.getDefaultconfig(rootSchema);
    defaultSchema = config.getDefaultSchema();
    operatorTable = config.getOperatorTable();
    parserConfig = config.getParserConfig();
    state = State.STATE_0_CLOSED;
    traitDefs = config.getTraitDefs();
    convertletTable = config.getConvertletTable();
    executor = config.getExecutor();
    reset();

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

  @Test
  public void calTest1() throws SqlParseException,
    ClassNotFoundException,
    SQLException {

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
    // Check logical plan
    String expectedResult = "LogicalProject(product_id=[$0], product_name=[$1], order_id=[$2])" +
      "  LogicalFilter(condition=[>($3, 2)])" +
      "    LogicalProject(id=[$0], name=[$1], id1=[$3], quantity=[$4])" +
      "      LogicalJoin(condition=[=($2, $5)], joinType=[inner])" +
      "        LogicalProject(id=[$0], name=[$1], id0=[CAST($0):VARCHAR CHARACTER SET \"ISO-8859-1\" COLLATE \"" +
      "ISO-8859-1$en_US$primary\"])" +
      "          EnumerableTableScan(table=[[t, products]])" +
      "        LogicalProject(id=[$0], quantity=[$1], id0=[CAST($0):VARCHAR CHARACTER SET \"ISO-8859-1\" COLLATE \"" +
      "ISO-8859-1$en_US$primary\"])" +
      "          EnumerableTableScan(table=[[t, orders]])";
    assertEquals(expectedResult,
      RelOptUtil.toString(root.rel).replaceAll(System.getProperty("line.separator"), ""));

    // Optimize the plan
    RelOptPlanner planner = new VolcanoPlanner();

    // Create a set of rules to apply
    Program program = Programs.ofRules(
      GearFilterRule.INSTANCE,
      LoptOptimizeJoinRule.INSTANCE);

    RelTraitSet traitSet = planner.emptyTraitSet().replace(EnumerableConvention.INSTANCE);

    // Execute the program
//    RelNode optimized = program.run(planner, root.rel, traitSet,
//      ImmutableList.<RelOptMaterialization>of(), ImmutableList.<RelOptLattice>of());
//    LOG.info(RelOptUtil.toString(optimized));

  }

  private Planner getPlanner(List<RelTraitDef> traitDefs,
                             SqlParser.Config parserConfig,
                             Program... programs) throws ClassNotFoundException, SQLException {

    Class.forName("org.apache.calcite.jdbc.Driver");
    java.sql.Connection connection = DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    rootSchema.add("T", new ReflectiveSchema(new SampleTransactions.Transactions()));

    final FrameworkConfig config = Frameworks.newConfigBuilder()
      .parserConfig(parserConfig)
      .defaultSchema(rootSchema)
      .traitDefs(traitDefs)
      .programs(programs)
      .build();
    return Frameworks.getPlanner(config);
  }

  @Test
  public void calTest2() throws SQLException,
    ClassNotFoundException,
    SqlParseException,
    ValidationException,
    RelConversionException {

    RuleSet ruleSet = RuleSets.ofList(
      SortRemoveRule.INSTANCE,
      EnumerableRules.ENUMERABLE_PROJECT_RULE,
      EnumerableRules.ENUMERABLE_SORT_RULE);
    Planner planner = getPlanner(traitDefs, SqlParser.Config.DEFAULT, Programs.of(ruleSet));

    String sql = "SELECT * FROM t.products ORDER BY t.products.id";

    SqlNode parse = planner.parse(sql);
    String expectedParseResult = "SELECT *FROM `T`.`PRODUCTS`ORDER BY `T`.`PRODUCTS`.`ID`";
    // Check SQL Parse Tree
    assertEquals(expectedParseResult,
      parse.toString().replaceAll(System.getProperty("line.separator"), ""));

    SqlNode validate = planner.validate(parse);
    RelNode convert = planner.rel(validate).project();
    RelTraitSet traitSet = convert.getTraitSet().replace(EnumerableConvention.INSTANCE);
    RelNode transform = planner.transform(0, traitSet, convert);
    // Check Relational Expression Result
    String expectedRelationalExpressionResult = "EnumerableSort(sort0=[$0], dir0=[ASC])" +
      "  EnumerableProject(ID=[$0], NAME=[$1])" +
      "    EnumerableTableScan(table=[[T, PRODUCTS]])";
    assertEquals(expectedRelationalExpressionResult,
      RelOptUtil.toString(transform).replaceAll(System.getProperty("line.separator"), ""));
  }

}
