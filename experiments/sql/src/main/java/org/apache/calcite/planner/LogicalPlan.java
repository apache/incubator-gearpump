package org.apache.calcite.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

public class LogicalPlan {

    public static RelNode getLogicalPlan(String query, Planner planner) throws ValidationException,
            RelConversionException {
        SqlNode sqlNode;

        try {
            sqlNode = planner.parse(query);
        } catch (SqlParseException e) {
            throw new RuntimeException("SQL query parsing error", e);
        }
        SqlNode validatedSqlNode = planner.validate(sqlNode);

        return planner.rel(validatedSqlNode).project();
    }
}
