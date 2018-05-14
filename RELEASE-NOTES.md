# ddth-dao release notes

## 0.8.5.1 - 2018-05-15

- `IJdbcHelper`: `executeSelectAsStream(...)` now supports auto closing the supplied `Connection`.


## 0.8.5 - 2018-05-14

- Update dependency libs.
- `AbstractGenericRowMapper`: add utility method to generate SQLs for SELECT, INSERT, DELETE, UPDATE.
- Bug fixes & Enhancements: `AbstractGenericBoJdbcDao` and `AbstractGenericRowMapper` now supports the typed parameter is a typed class (e.g. `MyRowMapper extends AbstractGenericRowMapper<AGeneticClass<T>>`).


## 0.8.4 - 2017-12-24

- Update dependency libs.
- `BaseDao`: in case of `CacheException`, operation should not fail:
  - Put, Remove: log exception with `warn` level.
  - Get: log exception with `warn` level and return `null`.


## 0.8.3 - 2017-11-04

- Support streaming of result from a SELECT query:
  - New class `ResultSetIterator`.
  - New methods `IJdbcHelper.executeSelectAsStream(...)`
- Refactoring & Enhancements:
  - New class `UniversalRowMapper`: implements `IRowMapper<Map<String, Object>>`
  - New helper class `JdbcHelper`
  - Both `DdthJdbcHelper` and `JdbcTemplateJdbcHelper` now support `IN` clause (named-parameters only)
- `BaseBo`: new methods `public Map<String, Object> getAttributes()` and `public BaseBo setAttributes(Map<String, Object>)`.
- New method and enum to detect db vendor: `DatabaseVendor` and `DbcHelper.detectDbVendor(Connection)`
- Other fixes and improvements


## 0.8.2 - 2017-10-24

- Exception:
  - Keep only `DaoException` and `DuplicatedValueException`
  - Remove `DuplicatedUniqueException` and `MissingValueException`
- Rename `DaoOperationStatus.DUPLICATED_KEY` to `DaoOperationStatus.DUPLICATED_VALUE`
- `DaoOperationStatus.DUPLICATED_UNIQUE` is now deprecated
- `AbstractJdbcHelper`: translates `SQLException` to `DaoException`
- New method `DbcHelper.getDataSource(Connection)`
- Fixes and enhancements


## 0.8.1.1 - 2017-10-20

- Add support for `checksum` column:
  - New annotation `ChecksumColumn`
  - `AbstractGenericRowMapper`, `AnnotatedGenericRowMapper` && `AbstractGenericBoJdbcDao`: support checksum column.
- `IGenericBoDao` && `AbstractGenericBoJdbcDao`: new methods `createOrUpdate(bo)` and `updateOrCreate(bo)`.
- `IJdbcHelper`, `AbstractJdbcHelper` and `BaseJdbcDao`: support multiple data-sources.
- `AbstractGenericBoJdbcDao`: add protected version of public methods.
- Fix: `AnnotatedGenericRowMapper.getAllColumns()` now returns columns in correct order.
- Fix: `AbstractJdbcHelper` bug that caused pre-opened connection to be closed.


## 0.8.0.4 - 2017-08-16

- Fixes/Enhancements:
  - `AbstractGenericBoJdbcDao`: protected method to retrieve `typeClass` member attribute in sub-classes.
  - `AbstractGenericRowMapper`:
    - Constructor: proper way to retrieve value for `typeClass`
    - Protected method to retrieve `typeClass` member attribute in sub-classes.
    - Provide a way for `mapRow(...)` to pass custom class loader to `BoUtils.createObject(...)`


## 0.8.0.3 - 2017-08-10

- `AbstractGenericRowMapper.ColAttrMapping`: attribute class can be either primitive or wrapper.
- `AbstractGenericBoJdbcDao` new methods to support data partitioning & custom queries:
  - `protected String calcTableName(BoId id)` and `protected String calcTableName(T bo)`
  - `protected String calcSqlInsert(BoId id)` and `protected String calcSqlInsert(T bo)`
  - `protected String calcSqlDeleteOne(BoId id)` and `protected String calcSqlDeleteOne(T bo)`
  - `protected String calcSqlSelectOne(BoId id)` and `protected String calcSqlSelectOne(T bo)`
  - `protected String calcSqlUpdateOne(BoId id)` and `protected String calcSqlUpdateOne(T bo)`
- `AbstractGenericBoJdbcDao`: various bugs fixed


## 0.8.0.1 - 2017-08-06

- `BaseBo`: new methods
  - `<T> Optional<T> getAttributeOptional(String attrName, Class<T> clazz)`
  - `Date getAttributeAsDate(String attrName, String dateTimeFormat)`
- New class `BaseDataJsonFieldBo`
- `BaseJsonBo`: new method
  - `<T> Optional<T> getSubAttrOptional(String attrName, String dPath, Class<T> clazz)`
- New class `BoId`.
- New interface `IGenericBoDao<T>`: API interface for DAO that manages one single BO class.
  - New class `AbstractGenericBoJdbcDao<T>`
- New class `AbstractGenericRowMapper<T>`
- New package `com.github.ddth.dao.jdbc.annotations`
- New classes `AbstractJdbcHelper` and `DdthJdbcHelper`
  - `DdthJdbcHelper`: pure-JDBC implementation (does not depends on Spring's `JdbcTemplate`)
  - Support binding of value array.
- `IJdbcHelper` now supports both index-based and name-based value bindings.
- `DbcHelper`: new method
  - `public static void bindParams(PreparedStatement pstm, Object... bindValues) throws SQLException`
- New exception classes:
  - `DaoException`
  - `DuplicatedKeyException`
  - `DuplicatedUniqueException`
  - `MissingValueException`
- `DdthJdbcHelper` and `JdbcTemplateJdbcHelper` now throw `DaoException` instead of `SQLException`
- `ParamExpression` is deprecated, wait for future rework.
- Update & Upgrade dependencies.
- Refactor & More unit tests.


## 0.7.1 - 2017-04-18

- Breaking change:
  - `BaseBo.getAttribute(String)` and `BaseBo.getAttribute(String, Class)` no longer delegates to `DPathUtils.getValue()` but to `MapUtils.getValue(...)`.
  - The same to `BaseBo.setAttribute(String, Object)`.
- `BaseBo.setAttribute(String key, Object value)`: if value is `null`, the entry is removed from attribute map.
- `BaseBo`: new methods (`attributeExists(String)`, `attributeMap()`, `removeAttribute(String)`, `triggerChange(String)`) and improvements.
- New class `BaseJsonBo`.


## 0.7.0 - 2017-02-03

- Refactor `BaseJdbcDao`:
  - Abstract from `JdbcTemplate` with new interfaces `IRowMapper` and `IJdbcHelper`.
  - New class `com.github.ddth.dao.jdbc.jdbctemplate.JdbcTemplateJdbcHelper`


## 0.6.0.3 - 2016-11-15

- Update dependencies.
- Minor fixes & enhancements.


## 0.6.0.1 - 2016-10-03

- Bump to `com.github.ddth:ddth-parent:6`, now requires Java 8+.
- `BaseBo` now implements `ISerializationSupport` interface.
- New utility class `BoUtils`.
- BREAKING CHANGE: `ProfilingRecord`'s structure changes:
  - `execTime`: UNIX timestamp (in milliseconds) when the command started.
  - `duration`: command's duration in milliseconds
- BREAKING CHANGE: `BaseBo`'s `toXXX()` and `fromXXX()` have changed their internal mechanism!
- Update dependencies.
- Minor fixes & enhancements.


## 0.5.0.5 - 2016-06-28

- `BaseBo` now implements interface `Cloneable`.


## 0.5.0.4 - 2016-04-22

- Minor enhancements: no more `synchronized` methods in class `BaseBo`.
- `BaseBo`: New method `protected void triggerPopulate()`


## 0.5.0.1 - 2015-10-09

- POM fixed.


## 0.5.0 - 2015-10-08

- Separate artifacts: `ddth-dao-core`, `ddth-dao-jdbc` and `ddth-dao-cassandra`.


## 0.4.0.2 - 2015-05-09

- Minor enhancements: new transaction-support methods for class `BaseJdbcDao`


## 0.4.0.1 - 2014-12-09

- Bugs fixed.


## 0.4.0 - 2014-12-01

- New methods in class `DbcHelper`:
- Bugs fixed.


## 0.3.2.1 - 2014-11-27

- Minor *QL profiling enhancements.


## 0.3.2 - 2014-11-18

- `BaseBo`: remove field `__dirty__` from serialized form.
- `WideRowJsonCassandraNosqlEngine`: bug fix in method `void store(String tableName, String entryId, Map<Object, Object> data)`


## 0.3.1 - 2014-11-15

- NoSQL DAO: new method `Collection<String> entryIdList(String storageId)`


## 0.3.0.1 - 2014-11-08

- New package(s) `com.github.ddth.dao.nosql.*` to support NoSQL-DAOs.
- Support NoSQL engine: `Apache Cassandra`.


## 0.2.1 - 2014-11-04

- Moved class `DbcHelper` to package `com.github.ddth.dao.jdbc`.
- `BaseJdbcDao`'s methods utilize `DbcHelper`.
- Changed parameters of `SqlHelper.buildSqlSELECT()`.


## 0.1.0 - 2014-11-02

- First release.
