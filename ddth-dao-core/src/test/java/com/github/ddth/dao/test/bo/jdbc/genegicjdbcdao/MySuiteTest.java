package com.github.ddth.dao.test.bo.jdbc.genegicjdbcdao;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)

@Suite.SuiteClasses({ JdbcTemplateGenericJdbcDaoTCase.class, DdthGenericJdbcDaoTCase.class })

/*
 * mvn test -DskipTests=false -Dtest=com.github.ddth.dao.test.bo.jdbc.genegicjdbcdao.MySuiteTest
 */
public class MySuiteTest {
}
