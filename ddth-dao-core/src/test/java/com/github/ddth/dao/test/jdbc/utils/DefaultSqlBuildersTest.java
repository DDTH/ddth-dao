package com.github.ddth.dao.test.jdbc.utils;

import com.github.ddth.dao.jdbc.utils.*;
import com.github.ddth.dao.utils.DatabaseVendor;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.junit.Assert;

import java.util.Map;
import java.util.TreeMap;

public class DefaultSqlBuildersTest extends TestCase {

    public DefaultSqlBuildersTest(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(DefaultSqlBuildersTest.class);
    }

    final static String TABLE1 = "tbl1";
    final static String TABLE2 = "tbl2";

    @org.junit.Test
    public void testDeleteBuilder1() {
        BuildSqlResult result = new DefaultSqlBuilders.DeleteBuilder(TABLE1, null).build();
        Assert.assertEquals("DELETE FROM " + TABLE1, result.clause);
        Assert.assertEquals(0, result.bindValues.length);
    }

    @org.junit.Test
    public void testDeleteBuilder2() {
        IFilter filter = new DefaultFilters.FilterFieldValue("col", "=", 123);
        BuildSqlResult result = new DefaultSqlBuilders.DeleteBuilder(TABLE1, filter).build();
        Assert.assertEquals("DELETE FROM " + TABLE1 + " WHERE col = ?", result.clause);
        Assert.assertEquals(1, result.bindValues.length);
    }

    @org.junit.Test
    public void testInsertBuilder1() {
        String columnName = "yob";
        int columnValue = 2000;
        BuildSqlResult result = new DefaultSqlBuilders.InsertBuilder(TABLE1).addValue(columnName, columnValue).build();
        Assert.assertEquals("INSERT INTO " + TABLE1 + " (" + columnName + ") VALUES (?)", result.clause);
        Assert.assertEquals(1, result.bindValues.length);
    }

    @org.junit.Test
    public void testInsertBuilder2() {
        String columnName = "yob";
        ParamRawExpression columnValue = new ParamRawExpression("2000");
        BuildSqlResult result = new DefaultSqlBuilders.InsertBuilder(TABLE1).addValue(columnName, columnValue).build();
        Assert.assertEquals("INSERT INTO " + TABLE1 + " (" + columnName + ") VALUES (2000)", result.clause);
        Assert.assertEquals(0, result.bindValues.length);
    }

    @org.junit.Test
    public void testSelectBuilder1() {
        IFilter filter = new DefaultFilters.FilterFieldValue("is_active", "=", "y");
        BuildSqlResult result = new DefaultSqlBuilders.SelectBuilder().withColumns("col1", "col2")
                .withFilterWhere(filter).withTables(TABLE1).build();
        Assert.assertEquals("SELECT col1,col2 FROM " + TABLE1 + " WHERE is_active = ?", result.clause);
        Assert.assertEquals(1, result.bindValues.length);
    }

    @org.junit.Test
    public void testSelectBuilderMySql() {
        IFilter filterWhere = new DefaultFilters.FilterExpression(new ParamRawExpression(TABLE1 + ".id"), "=",
                new ParamRawExpression(TABLE2 + ".id"));
        IFilter filterHaving = new DefaultFilters.FilterFieldValue("SUM(" + TABLE2 + ".products)", ">=", 1024);

        ISqlBuilder builder = new DefaultSqlBuilders.SelectBuilder()
                .withColumns(TABLE1 + ".col1", TABLE2 + ".col2", "SUM(" + TABLE2 + ".products)")
                .withFilterWhere(filterWhere).withGroupByColumns(TABLE1 + ".yob").withFilterHaving(filterHaving)
                .addSorting(TABLE1 + ".id", false).withVendor(DatabaseVendor.MYSQL).withTables(TABLE1, TABLE2);
        {
            BuildSqlResult result = ((DefaultSqlBuilders.SelectBuilder) builder).withLimit(10).build();
            Assert.assertEquals(
                    "SELECT " + TABLE1 + ".col1," + TABLE2 + ".col2,SUM(" + TABLE2 + ".products) FROM " + TABLE1 + ","
                            + TABLE2 + " WHERE " + TABLE1 + ".id = " + TABLE2 + ".id GROUP BY " + TABLE1
                            + ".yob HAVING SUM(" + TABLE2 + ".products) >= ? ORDER BY " + TABLE1 + ".id ASC LIMIT 10",
                    result.clause);
            Assert.assertEquals(1, result.bindValues.length);
        }
        {
            BuildSqlResult result = ((DefaultSqlBuilders.SelectBuilder) builder).withLimit(4, 2).build();
            Assert.assertEquals(
                    "SELECT " + TABLE1 + ".col1," + TABLE2 + ".col2,SUM(" + TABLE2 + ".products) FROM " + TABLE1 + ","
                            + TABLE2 + " WHERE " + TABLE1 + ".id = " + TABLE2 + ".id GROUP BY " + TABLE1
                            + ".yob HAVING SUM(" + TABLE2 + ".products) >= ? ORDER BY " + TABLE1 + ".id ASC LIMIT 2,4",
                    result.clause);
            Assert.assertEquals(1, result.bindValues.length);
        }
    }

    @org.junit.Test
    public void testSelectBuilderPgSql() {
        IFilter filterWhere = new DefaultFilters.FilterExpression(new ParamRawExpression(TABLE1 + ".id"), "=",
                new ParamRawExpression(TABLE2 + ".id"));
        IFilter filterHaving = new DefaultFilters.FilterFieldValue("SUM(" + TABLE2 + ".products)", ">=", 1024);

        ISqlBuilder builder = new DefaultSqlBuilders.SelectBuilder()
                .withColumns(TABLE1 + ".col1", TABLE2 + ".col2", "SUM(" + TABLE2 + ".products)")
                .withFilterWhere(filterWhere).withGroupByColumns(TABLE1 + ".yob").withFilterHaving(filterHaving)
                .addSorting(TABLE1 + ".id", false).withVendor(DatabaseVendor.POSTGRESQL).withTables(TABLE1, TABLE2);
        {
            BuildSqlResult result = ((DefaultSqlBuilders.SelectBuilder) builder).withLimit(10).build();
            Assert.assertEquals(
                    "SELECT " + TABLE1 + ".col1," + TABLE2 + ".col2,SUM(" + TABLE2 + ".products) FROM " + TABLE1 + ","
                            + TABLE2 + " WHERE " + TABLE1 + ".id = " + TABLE2 + ".id GROUP BY " + TABLE1
                            + ".yob HAVING SUM(" + TABLE2 + ".products) >= ? ORDER BY " + TABLE1 + ".id ASC LIMIT 10",
                    result.clause);
            Assert.assertEquals(1, result.bindValues.length);
        }
        {
            BuildSqlResult result = ((DefaultSqlBuilders.SelectBuilder) builder).withLimit(4, 2).build();
            Assert.assertEquals(
                    "SELECT " + TABLE1 + ".col1," + TABLE2 + ".col2,SUM(" + TABLE2 + ".products) FROM " + TABLE1 + ","
                            + TABLE2 + " WHERE " + TABLE1 + ".id = " + TABLE2 + ".id GROUP BY " + TABLE1
                            + ".yob HAVING SUM(" + TABLE2 + ".products) >= ? ORDER BY " + TABLE1
                            + ".id ASC LIMIT 4 OFFSET 2", result.clause);
            Assert.assertEquals(1, result.bindValues.length);
        }
    }

    @org.junit.Test
    public void testSelectBuilderOracle() {
        IFilter filterWhere = new DefaultFilters.FilterExpression(new ParamRawExpression(TABLE1 + ".id"), "=",
                new ParamRawExpression(TABLE2 + ".id"));
        IFilter filterHaving = new DefaultFilters.FilterFieldValue("SUM(" + TABLE2 + ".products)", ">=", 1024);

        ISqlBuilder builder = new DefaultSqlBuilders.SelectBuilder()
                .withColumns(TABLE1 + ".col1", TABLE2 + ".col2", "SUM(" + TABLE2 + ".products)")
                .withFilterWhere(filterWhere).withGroupByColumns(TABLE1 + ".yob").withFilterHaving(filterHaving)
                .addSorting(TABLE1 + ".id", false).withVendor(DatabaseVendor.ORACLE).withTables(TABLE1, TABLE2);
        {
            BuildSqlResult result = ((DefaultSqlBuilders.SelectBuilder) builder).withLimit(10).build();
            Assert.assertEquals(
                    "SELECT " + TABLE1 + ".col1," + TABLE2 + ".col2,SUM(" + TABLE2 + ".products) FROM " + TABLE1 + ","
                            + TABLE2 + " WHERE " + TABLE1 + ".id = " + TABLE2 + ".id GROUP BY " + TABLE1
                            + ".yob HAVING SUM(" + TABLE2 + ".products) >= ? ORDER BY " + TABLE1
                            + ".id ASC FETCH NEXT 10 ROWS ONLY", result.clause);
            Assert.assertEquals(1, result.bindValues.length);
        }
        {
            BuildSqlResult result = ((DefaultSqlBuilders.SelectBuilder) builder).withLimit(4, 2).build();
            Assert.assertEquals(
                    "SELECT " + TABLE1 + ".col1," + TABLE2 + ".col2,SUM(" + TABLE2 + ".products) FROM " + TABLE1 + ","
                            + TABLE2 + " WHERE " + TABLE1 + ".id = " + TABLE2 + ".id GROUP BY " + TABLE1
                            + ".yob HAVING SUM(" + TABLE2 + ".products) >= ? ORDER BY " + TABLE1
                            + ".id ASC OFFSET 2 ROWS FETCH NEXT 4 ROWS ONLY", result.clause);
            Assert.assertEquals(1, result.bindValues.length);
        }
    }

    @org.junit.Test
    public void testSelectBuilderMssql() {
        IFilter filterWhere = new DefaultFilters.FilterExpression(new ParamRawExpression(TABLE1 + ".id"), "=",
                new ParamRawExpression(TABLE2 + ".id"));
        IFilter filterHaving = new DefaultFilters.FilterFieldValue("SUM(" + TABLE2 + ".products)", ">=", 1024);

        ISqlBuilder builder = new DefaultSqlBuilders.SelectBuilder()
                .withColumns(TABLE1 + ".col1", TABLE2 + ".col2", "SUM(" + TABLE2 + ".products)")
                .withFilterWhere(filterWhere).withGroupByColumns(TABLE1 + ".yob").withFilterHaving(filterHaving)
                .addSorting(TABLE1 + ".id", false).withVendor(DatabaseVendor.MSSQL).withTables(TABLE1, TABLE2);
        {
            BuildSqlResult result = ((DefaultSqlBuilders.SelectBuilder) builder).withLimit(10).build();
            Assert.assertEquals(
                    "SELECT " + TABLE1 + ".col1," + TABLE2 + ".col2,SUM(" + TABLE2 + ".products) FROM " + TABLE1 + ","
                            + TABLE2 + " WHERE " + TABLE1 + ".id = " + TABLE2 + ".id GROUP BY " + TABLE1
                            + ".yob HAVING SUM(" + TABLE2 + ".products) >= ? ORDER BY " + TABLE1
                            + ".id ASC FETCH NEXT 10 ROWS ONLY", result.clause);
            Assert.assertEquals(1, result.bindValues.length);
        }
        {
            BuildSqlResult result = ((DefaultSqlBuilders.SelectBuilder) builder).withLimit(4, 2).build();
            Assert.assertEquals(
                    "SELECT " + TABLE1 + ".col1," + TABLE2 + ".col2,SUM(" + TABLE2 + ".products) FROM " + TABLE1 + ","
                            + TABLE2 + " WHERE " + TABLE1 + ".id = " + TABLE2 + ".id GROUP BY " + TABLE1
                            + ".yob HAVING SUM(" + TABLE2 + ".products) >= ? ORDER BY " + TABLE1
                            + ".id ASC OFFSET 2 ROWS FETCH NEXT 4 ROWS ONLY", result.clause);
            Assert.assertEquals(1, result.bindValues.length);
        }
    }

    @org.junit.Test
    public void testUpdateBuilder1() {
        Map<String, Object> fieldsAndValues = new TreeMap<>();
        fieldsAndValues.put("yob", 2000);
        fieldsAndValues.put("email", "email@domain.com");
        fieldsAndValues.put("notes", new ParamRawExpression("'This is a string'"));
        IFilter filter = new DefaultFilters.FilterFieldValue("id", "=", "myid");
        BuildSqlResult result = new DefaultSqlBuilders.UpdateBuilder(TABLE1).withValues(fieldsAndValues)
                .withFilter(filter).build();
        Assert.assertEquals("UPDATE " + TABLE1 + " SET email=?,notes='This is a string',yob=? WHERE id = ?",
                result.clause);
        Assert.assertEquals(3, result.bindValues.length);
    }
}
