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

package org.apache.gearpump.experiments.sql

import java.util

import org.apache.calcite.config.Lex
import org.apache.calcite.plan.{Contexts, ConventionTraitDef, RelTrait, RelTraitDef}
import org.apache.calcite.rel.`type`.RelDataTypeSystem
import org.apache.calcite.rel.{RelCollationTraitDef, RelNode}
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.tools._

object Query {

  def getLogicalPlan(query: String): RelNode = {

    val traitDefs: util.List[RelTraitDef[_ <: RelTrait]] = new util.ArrayList[RelTraitDef[_ <: RelTrait]]

    traitDefs.add(ConventionTraitDef.INSTANCE)
    traitDefs.add(RelCollationTraitDef.INSTANCE)

    val config = Frameworks.newConfigBuilder()
      .parserConfig(SqlParser.configBuilder.setLex(Lex.MYSQL).build)
      .traitDefs(traitDefs)
      .context(Contexts.EMPTY_CONTEXT)
      .ruleSets(RuleSets.ofList())
      .costFactory(null)
      .typeSystem(RelDataTypeSystem.DEFAULT)
      .build();

    val queryPlanner = Frameworks.getPlanner(config)
    val sqlNode = queryPlanner.parse(query)
    val validatedSqlNode = queryPlanner.validate(sqlNode)

    queryPlanner.rel(validatedSqlNode).project()
  }

}
