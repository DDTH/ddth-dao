package com.github.ddth.dao.test.jdbc.utils;

import com.github.ddth.dao.jdbc.utils.BuildNamedParamsSqlResult;
import com.github.ddth.dao.jdbc.utils.DefaultNamedParamsFilters;
import com.github.ddth.dao.jdbc.utils.INamedParamsFilter;
import com.github.ddth.dao.jdbc.utils.ParamRawExpression;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.junit.Assert;

public class DefaultNamedParamsFiltersTest extends TestCase {

    public DefaultNamedParamsFiltersTest(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(DefaultNamedParamsFiltersTest.class);
    }

    @org.junit.Test
    public void testFilterOr1a() {
        INamedParamsFilter filter = new DefaultNamedParamsFilters.FilterOr()
                .addFilter(new DefaultNamedParamsFilters.FilterFieldValue("yob", "  <   ", 2000))
                .addFilter(new DefaultNamedParamsFilters.FilterFieldValue("type", "  =   ", "email"));
        BuildNamedParamsSqlResult result = filter.build();
        Assert.assertEquals("(yob < :yob) OR (type = :type)", result.clause);
        Assert.assertEquals(2, result.bindValues.size());
    }

    @org.junit.Test
    public void testFilterOr1b() {
        INamedParamsFilter filter = new DefaultNamedParamsFilters.FilterOr()
                .addFilter(new DefaultNamedParamsFilters.FilterFieldValue("yob\rY", "  <   ", 2000))
                .addFilter(new DefaultNamedParamsFilters.FilterFieldValue("type\nT", "  =   ", "email"));
        BuildNamedParamsSqlResult result = filter.build();
        Assert.assertEquals("(yob < :Y) OR (type = :T)", result.clause);
        Assert.assertEquals(2, result.bindValues.size());
    }

    @org.junit.Test
    public void testFilterOr2a() {
        INamedParamsFilter filter = new DefaultNamedParamsFilters.FilterOr().addFilter(
                new DefaultNamedParamsFilters.FilterFieldValue("yob", "  =   ", new ParamRawExpression("2000")))
                .addFilter(new DefaultNamedParamsFilters.FilterFieldValue("type", "  !=   ", "email"));
        BuildNamedParamsSqlResult result = filter.build();
        Assert.assertEquals("(yob = 2000) OR (type != :type)", result.clause);
        Assert.assertEquals(1, result.bindValues.size());
    }

    @org.junit.Test
    public void testFilterOr2b() {
        INamedParamsFilter filter = new DefaultNamedParamsFilters.FilterOr().addFilter(
                new DefaultNamedParamsFilters.FilterFieldValue("yob\tYOB", "  =   ", new ParamRawExpression("2000")))
                .addFilter(new DefaultNamedParamsFilters.FilterFieldValue("type\tTYPE", "  !=   ", "email"));
        BuildNamedParamsSqlResult result = filter.build();
        Assert.assertEquals("(yob = 2000) OR (type != :TYPE)", result.clause);
        Assert.assertEquals(1, result.bindValues.size());
    }

    @org.junit.Test
    public void testFilterAnd1a() {
        INamedParamsFilter filter = new DefaultNamedParamsFilters.FilterAnd()
                .addFilter(new DefaultNamedParamsFilters.FilterFieldValue("yob", "  <   ", 2000))
                .addFilter(new DefaultNamedParamsFilters.FilterFieldValue("type", "  =   ", "email"));
        BuildNamedParamsSqlResult result = filter.build();
        Assert.assertEquals("(yob < :yob) AND (type = :type)", result.clause);
        Assert.assertEquals(2, result.bindValues.size());
    }

    @org.junit.Test
    public void testFilterAnd1b() {
        INamedParamsFilter filter = new DefaultNamedParamsFilters.FilterAnd()
                .addFilter(new DefaultNamedParamsFilters.FilterFieldValue("yob\u0001yyy", "  <   ", 2000))
                .addFilter(new DefaultNamedParamsFilters.FilterFieldValue("type\u0002ttt", "  =   ", "email"));
        BuildNamedParamsSqlResult result = filter.build();
        Assert.assertEquals("(yob < :yyy) AND (type = :ttt)", result.clause);
        Assert.assertEquals(2, result.bindValues.size());
    }

    @org.junit.Test
    public void testFilterAnd2a() {
        INamedParamsFilter filter = new DefaultNamedParamsFilters.FilterAnd().addFilter(
                new DefaultNamedParamsFilters.FilterFieldValue("yob", "  =   ", new ParamRawExpression("2000")))
                .addFilter(new DefaultNamedParamsFilters.FilterFieldValue("type", "  !=   ", "email"));
        BuildNamedParamsSqlResult result = filter.build();
        Assert.assertEquals("(yob = 2000) AND (type != :type)", result.clause);
        Assert.assertEquals(1, result.bindValues.size());
    }

    @org.junit.Test
    public void testFilterAnd2b() {
        INamedParamsFilter filter = new DefaultNamedParamsFilters.FilterAnd().addFilter(
                new DefaultNamedParamsFilters.FilterFieldValue("yob\u0003yob", "  =   ",
                        new ParamRawExpression("2000")))
                .addFilter(new DefaultNamedParamsFilters.FilterFieldValue("type\u0004Type", "  !=   ", "email"));
        BuildNamedParamsSqlResult result = filter.build();
        Assert.assertEquals("(yob = 2000) AND (type != :Type)", result.clause);
        Assert.assertEquals(1, result.bindValues.size());
    }

    @org.junit.Test
    public void testFilterOrAnd1a() {
        INamedParamsFilter filter = new DefaultNamedParamsFilters.FilterOr()
                .addFilter(new DefaultNamedParamsFilters.FilterFieldValue("yob", "  <   ", 2000)).addFilter(
                        new DefaultNamedParamsFilters.FilterAnd()
                                .addFilter(new DefaultNamedParamsFilters.FilterFieldValue("yob", "  =   ", 2001))
                                .addFilter(new DefaultNamedParamsFilters.FilterFieldValue("type", "  =   ", "email")));
        BuildNamedParamsSqlResult result = filter.build();
        Assert.assertEquals("(yob < :yob) OR ((yob = :yob) AND (type = :type))", result.clause);
        Assert.assertEquals(2, result.bindValues.size());
    }

    @org.junit.Test
    public void testFilterOrAnd1b() {
        INamedParamsFilter filter = new DefaultNamedParamsFilters.FilterOr()
                .addFilter(new DefaultNamedParamsFilters.FilterFieldValue("yob\u0005yob1", "  <   ", 2000)).addFilter(
                        new DefaultNamedParamsFilters.FilterAnd().addFilter(
                                new DefaultNamedParamsFilters.FilterFieldValue("yob\u0006yob2", "  =   ", 2001))
                                .addFilter(new DefaultNamedParamsFilters.FilterFieldValue("type\u0007type0", "  =   ",
                                        "email")));
        BuildNamedParamsSqlResult result = filter.build();
        Assert.assertEquals("(yob < :yob1) OR ((yob = :yob2) AND (type = :type0))", result.clause);
        Assert.assertEquals(3, result.bindValues.size());
    }

    @org.junit.Test
    public void testFilterAndOrA() {
        INamedParamsFilter filter = new DefaultNamedParamsFilters.FilterAnd()
                .addFilter(new DefaultNamedParamsFilters.FilterFieldValue("yob", "  !=   ", 2000)).addFilter(
                        new DefaultNamedParamsFilters.FilterOr().addFilter(
                                new DefaultNamedParamsFilters.FilterFieldValue("yob", "  <   ",
                                        new ParamRawExpression("2000"))).addFilter(
                                new DefaultNamedParamsFilters.FilterFieldValue("type", "  !=   ",
                                        new ParamRawExpression("'email'"))));
        BuildNamedParamsSqlResult result = filter.build();
        Assert.assertEquals("(yob != :yob) AND ((yob < 2000) OR (type != 'email'))", result.clause);
        Assert.assertEquals(1, result.bindValues.size());
    }

    @org.junit.Test
    public void testFilterAndOrB() {
        INamedParamsFilter filter = new DefaultNamedParamsFilters.FilterAnd()
                .addFilter(new DefaultNamedParamsFilters.FilterFieldValue("yob\u0009YOB", "  !=   ", 2000)).addFilter(
                        new DefaultNamedParamsFilters.FilterOr().addFilter(
                                new DefaultNamedParamsFilters.FilterFieldValue("yob", "  <   ",
                                        new ParamRawExpression("2000"))).addFilter(
                                new DefaultNamedParamsFilters.FilterFieldValue("type", "  !=   ",
                                        new ParamRawExpression("'email'"))));
        BuildNamedParamsSqlResult result = filter.build();
        Assert.assertEquals("(yob != :YOB) AND ((yob < 2000) OR (type != 'email'))", result.clause);
        Assert.assertEquals(1, result.bindValues.size());
    }

    @org.junit.Test
    public void testFilterFieldValue1a() {
        INamedParamsFilter filter = new DefaultNamedParamsFilters.FilterFieldValue("colA", "  =   ", 1);
        BuildNamedParamsSqlResult result = filter.build();
        Assert.assertEquals("colA = :colA", result.clause);
        Assert.assertEquals(1, result.bindValues.size());
    }

    @org.junit.Test
    public void testFilterFieldValue1b() {
        INamedParamsFilter filter = new DefaultNamedParamsFilters.FilterFieldValue("colA\u001fcola", "  =   ", 1);
        BuildNamedParamsSqlResult result = filter.build();
        Assert.assertEquals("colA = :cola", result.clause);
        Assert.assertEquals(1, result.bindValues.size());
    }

    @org.junit.Test
    public void testFilterFieldValue2a() {
        INamedParamsFilter filter = new DefaultNamedParamsFilters.FilterFieldValue("colB", "<>",
                new ParamRawExpression("true"));
        BuildNamedParamsSqlResult result = filter.build();
        Assert.assertEquals("colB <> true", result.clause);
        Assert.assertEquals(0, result.bindValues.size());
    }

    @org.junit.Test
    public void testFilterFieldValue2b() {
        INamedParamsFilter filter = new DefaultNamedParamsFilters.FilterFieldValue("colB\u001ecolb", "<>",
                new ParamRawExpression("true"));
        BuildNamedParamsSqlResult result = filter.build();
        Assert.assertEquals("colB <> true", result.clause);
        Assert.assertEquals(0, result.bindValues.size());
    }

    @org.junit.Test
    public void testFilterExpression1() {
        INamedParamsFilter filter = new DefaultNamedParamsFilters.FilterExpression("left", "leftVal", "  =   ", "right",
                "rightVal");
        BuildNamedParamsSqlResult result = filter.build();
        Assert.assertEquals(":left = :right", result.clause);
        Assert.assertEquals(2, result.bindValues.size());
    }

    @org.junit.Test
    public void testFilterExpression2() {
        INamedParamsFilter filter = new DefaultNamedParamsFilters.FilterExpression(null,
                new ParamRawExpression("leftVal"), "!=", "RIGHT", "rightVal");
        BuildNamedParamsSqlResult result = filter.build();
        Assert.assertEquals("leftVal != :RIGHT", result.clause);
        Assert.assertEquals(1, result.bindValues.size());
    }
}
