ddth-dao release notes
======================

0.5.0.1 - 2015-10-09
--------------------

- POM fixed.


0.5.0 - 2015-10-08
------------------

- Separate artifacts: `ddth-dao-core`, `ddth-dao-jdbc` and `ddth-dao-cassandra`.


0.4.0.2 - 2015-05-09
--------------------

- Minor enhancements: new transaction-support methods for class `BaseJdbcDao`


0.4.0.1 - 2014-12-09
--------------------

- Bugs fixed.


0.4.0 - 2014-12-01
------------------

- New methods in class `DbcHelper`:
- Bugs fixed.


0.3.2.1 - 2014-11-27
--------------------

- Minor *QL profiling enhancements.


0.3.2 - 2014-11-18
------------------

- `BaseBo`: remove field `__dirty__` from serialized form.
- `WideRowJsonCassandraNosqlEngine`: bug fix in method `void store(String tableName, String entryId, Map<Object, Object> data)`


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
