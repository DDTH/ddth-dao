package com.github.ddth.dao.test.jdbc.utils;

import com.github.ddth.dao.jdbc.utils.BuildSqlResult;
import com.github.ddth.dao.jdbc.utils.DefaultFilters;
import com.github.ddth.dao.jdbc.utils.IFilter;
import com.github.ddth.dao.jdbc.utils.ParamRawExpression;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.junit.Assert;

public class DefaultFiltersTest extends TestCase {

    public DefaultFiltersTest(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(DefaultFiltersTest.class);
    }

    @org.junit.Test
    public void testFilterOr1() {
        IFilter filter = new DefaultFilters.FilterOr()
                .addFilter(new DefaultFilters.FilterFieldValue("yob", "  <   ", 2000))
                .addFilter(new DefaultFilters.FilterFieldValue("type", "  =   ", "email"));
        BuildSqlResult result = filter.build();
        Assert.assertEquals("(yob < ?) OR (type = ?)", result.clause);
        Assert.assertEquals(2, result.bindValues.length);
    }

    @org.junit.Test
    public void testFilterOr2() {
        IFilter filter = new DefaultFilters.FilterOr()
                .addFilter(new DefaultFilters.FilterFieldValue("yob", "  =   ", new ParamRawExpression("2000")))
                .addFilter(new DefaultFilters.FilterFieldValue("type", "  !=   ", "email"));
        BuildSqlResult result = filter.build();
        Assert.assertEquals("(yob = 2000) OR (type != ?)", result.clause);
        Assert.assertEquals(1, result.bindValues.length);
    }

    @org.junit.Test
    public void testFilterAnd1() {
        IFilter filter = new DefaultFilters.FilterAnd()
                .addFilter(new DefaultFilters.FilterFieldValue("yob", "  <   ", 2000))
                .addFilter(new DefaultFilters.FilterFieldValue("type", "  =   ", "email"));
        BuildSqlResult result = filter.build();
        Assert.assertEquals("(yob < ?) AND (type = ?)", result.clause);
        Assert.assertEquals(2, result.bindValues.length);
    }

    @org.junit.Test
    public void testFilterAnd2() {
        IFilter filter = new DefaultFilters.FilterAnd()
                .addFilter(new DefaultFilters.FilterFieldValue("yob", "  =   ", new ParamRawExpression("2000")))
                .addFilter(new DefaultFilters.FilterFieldValue("type", "  !=   ", "email"));
        BuildSqlResult result = filter.build();
        Assert.assertEquals("(yob = 2000) AND (type != ?)", result.clause);
        Assert.assertEquals(1, result.bindValues.length);
    }

    @org.junit.Test
    public void testFilterOrAnd() {
        IFilter filter = new DefaultFilters.FilterOr()
                .addFilter(new DefaultFilters.FilterFieldValue("yob", "  <   ", 2000)).addFilter(
                        new DefaultFilters.FilterAnd()
                                .addFilter(new DefaultFilters.FilterFieldValue("yob", "  =   ", 2000))
                                .addFilter(new DefaultFilters.FilterFieldValue("type", "  =   ", "email")));
        BuildSqlResult result = filter.build();
        Assert.assertEquals("(yob < ?) OR ((yob = ?) AND (type = ?))", result.clause);
        Assert.assertEquals(3, result.bindValues.length);
    }

    @org.junit.Test
    public void testFilterAndOr() {
        IFilter filter = new DefaultFilters.FilterAnd()
                .addFilter(new DefaultFilters.FilterFieldValue("yob", "  !=   ", 2000)).addFilter(
                        new DefaultFilters.FilterOr().addFilter(
                                new DefaultFilters.FilterFieldValue("yob", "  <   ", new ParamRawExpression("2000")))
                                .addFilter(new DefaultFilters.FilterFieldValue("type", "  !=   ",
                                        new ParamRawExpression("'email'"))));
        BuildSqlResult result = filter.build();
        Assert.assertEquals("(yob != ?) AND ((yob < 2000) OR (type != 'email'))", result.clause);
        Assert.assertEquals(1, result.bindValues.length);
    }

    @org.junit.Test
    public void testFilterFieldValue1() {
        IFilter filter = new DefaultFilters.FilterFieldValue("colA", "  =   ", 1);
        BuildSqlResult result = filter.build();
        Assert.assertEquals("colA = ?", result.clause);
        Assert.assertEquals(1, result.bindValues.length);
    }

    @org.junit.Test
    public void testFilterFieldValue2() {
        IFilter filter = new DefaultFilters.FilterFieldValue("colB", "<>", new ParamRawExpression("true"));
        BuildSqlResult result = filter.build();
        Assert.assertEquals("colB <> true", result.clause);
        Assert.assertEquals(0, result.bindValues.length);
    }

    @org.junit.Test
    public void testFilterExpression1() {
        IFilter filter = new DefaultFilters.FilterExpression("left", "  =   ", "right");
        BuildSqlResult result = filter.build();
        Assert.assertEquals("? = ?", result.clause);
        Assert.assertEquals(2, result.bindValues.length);
    }

    @org.junit.Test
    public void testFilterExpression2() {
        IFilter filter = new DefaultFilters.FilterExpression(new ParamRawExpression("left"), "!=", "right");
        BuildSqlResult result = filter.build();
        Assert.assertEquals("left != ?", result.clause);
        Assert.assertEquals(1, result.bindValues.length);
    }
}
