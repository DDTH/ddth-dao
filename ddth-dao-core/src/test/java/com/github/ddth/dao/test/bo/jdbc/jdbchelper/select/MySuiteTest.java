package com.github.ddth.dao.test.bo.jdbc.jdbchelper.select;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)

@Suite.SuiteClasses({ JdbcTemplateJdbcHelperTCase.class, DdthJdbcHelperTCase.class })

/*
 * mvn test -DskipTests=false -Dtest=com.github.ddth.dao.test.bo.jdbc.jdbchelper.select.MySuiteTest
 */
public class MySuiteTest {
}
