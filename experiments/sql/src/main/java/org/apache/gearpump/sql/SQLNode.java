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

package org.apache.gearpump.sql;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Util;
import org.junit.ComparisonFailure;

import java.util.regex.Pattern;

public class SQLNode {

  private static final Pattern LINE_BREAK_PATTERN = Pattern.compile("\r\n|\r|\n");

  private static final Pattern TAB_PATTERN = Pattern.compile("\t");

  private static final String LINE_BREAK = "\\\\n\"" + Util.LINE_SEPARATOR + " + \"";

  private static final ThreadLocal<boolean[]> LINUXIFY = new ThreadLocal<boolean[]>() {
    @Override
    protected boolean[] initialValue() {
      return new boolean[]{true};
    }
  };


  protected SqlParser getSqlParser(String sql) {
    return SqlParser.create(sql,
      SqlParser.configBuilder()
        .setParserFactory(SqlParserImpl.FACTORY)
        .setQuoting(Quoting.DOUBLE_QUOTE)
        .setUnquotedCasing(Casing.TO_UPPER)
        .setQuotedCasing(Casing.UNCHANGED)
        .setConformance(SqlConformanceEnum.DEFAULT)
        .build());
  }

  public static String toJavaString(String s) {
    s = Util.replace(s, "\"", "\\\"");
    s = LINE_BREAK_PATTERN.matcher(s).replaceAll(LINE_BREAK);
    s = TAB_PATTERN.matcher(s).replaceAll("\\\\t");
    s = "\"" + s + "\"";
    String spurious = "\n \\+ \"\"";
    if (s.endsWith(spurious)) {
      s = s.substring(0, s.length() - spurious.length());
    }
    return s;
  }

  public static void assertEqualsVerbose(String expected, String actual) {
    if (actual == null) {
      if (expected == null) {
        return;
      } else {
        String message = "Expected:\n" + expected + "\nActual: null";
        throw new ComparisonFailure(message, expected, null);
      }
    }
    if ((expected != null) && expected.equals(actual)) {
      return;
    }
    String s = toJavaString(actual);
    String message = "Expected:\n" + expected + "\nActual:\n" +
      actual + "\nActual java:\n" + s + '\n';

    throw new ComparisonFailure(message, expected, actual);
  }

  public void check(String sql, String expected) {
    final SqlNode sqlNode;
    try {
      sqlNode = getSqlParser(sql).parseStmt();
    } catch (SqlParseException e) {
      throw new RuntimeException("Error while parsing SQL: " + sql, e);
    }

    String actual = sqlNode.toSqlString(null, true).getSql();
    if (LINUXIFY.get()[0]) {
      actual = Util.toLinux(actual);
    }
    assertEqualsVerbose(expected, actual);
  }

  public void validateCall(SqlCall call, SqlValidator validator, SqlValidatorScope operandScope) {
    SqlCase sqlCase = (SqlCase) call;
    SqlNodeList whenOperands = sqlCase.getWhenOperands();
    SqlNodeList thenOperands = sqlCase.getThenOperands();
    SqlNode elseOperand = sqlCase.getElseOperand();
    for (SqlNode operand : whenOperands) {
      operand.validateExpr(validator, operandScope);
    }
    for (SqlNode operand : thenOperands) {
      operand.validateExpr(validator, operandScope);
    }
    if (elseOperand != null) {
      elseOperand.validateExpr(validator, operandScope);
    }
  }

}