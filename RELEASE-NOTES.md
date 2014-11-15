ddth-dao release notes
======================

0.3.1 - 2014-11-15
------------------
- NoSQL DAO: new method `Collection<String> entryIdList(String storageId)`


0.3.0.1 - 2014-11-08
--------------------
- New package(s) `com.github.ddth.dao.nosql.*` to support NoSQL-DAOs.
- Support NoSQL engine: `Apache Cassandra`.


0.2.1 - 2014-11-04
------------------
- Moved class `DbcHelper` to package `com.github.ddth.dao.jdbc`.
- `BaseJdbcDao`'s methods utilize `DbcHelper`.
- Changed parameters of `SqlHelper.buildSqlSELECT()`.


0.1.0 - 2014-11-02
------------------
- First release.
