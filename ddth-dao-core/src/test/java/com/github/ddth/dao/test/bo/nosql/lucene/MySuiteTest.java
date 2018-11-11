package com.github.ddth.dao.test.bo.nosql.lucene;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)

@Suite.SuiteClasses({ KdLuceneTest.class, KvLuceneTest.class })

/*
 * mvn test -DskipTests=false -Dtest=com.github.ddth.dao.test.bo.nosql.lucene.MySuiteTest
 */
public class MySuiteTest {
}
