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

import com.google.common.io.Resources;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;

public class QueryTest {

  @Test
  public void testLogicalPlan() throws IOException,
    SQLException,
    ValidationException,
    RelConversionException {

    CalciteConnection connection = new Connection();
    String salesSchema = Resources.toString(Query.class.getResource("/model.json"),
      Charset.defaultCharset());
    new ModelHandler(connection, "inline:" + salesSchema);

    Query queryPlanner = new Query(connection.getRootSchema().getSubSchema(connection.getSchema()));
    RelNode logicalPlan = queryPlanner.getLogicalPlan("SELECT item FROM transactions");

    String expectedLogicalPlan = "LogicalProject(item=[$2])" +
      "  LogicalTableScan(table=[[SALES, Transactions]])";
    assertEquals(expectedLogicalPlan,
      RelOptUtil.toString(logicalPlan).replaceAll(System.getProperty("line.separator"), ""));

  }
}
