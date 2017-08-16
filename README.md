[![Build Status](https://travis-ci.org/DDTH/ddth-dao.svg?branch=master)](https://travis-ci.org/DDTH/ddth-dao)

# ddth-dao

DDTH's DAO library.

By Thanh Ba Nguyen (btnguyen2k (at) gmail.com).

Project home:
[https://github.com/DDTH/ddth-dao](https://github.com/DDTH/ddth-dao)

**`ddth-dao` requires Java 8+ since v0.6.0.1**


## License

See LICENSE.txt for details. Copyright (c) 2014-2017 Thanh Ba Nguyen.

Third party libraries are distributed under their own license(s).


## Installation

Latest release version: `0.8.0.4`. See [RELEASE-NOTES.md](RELEASE-NOTES.md).

Maven dependency: if only a sub-set of `ddth-dao` functionality is used, choose the corresponding
dependency artifact(s) to reduce the number of unused jar files.

*ddth-dao-core*: core classes for DAO pattern, all other dependencies are *optional*.

```xml
<dependency>
    <groupId>com.github.ddth</groupId>
    <artifactId>ddth-dao-core</artifactId>
    <version>0.8.0.4</version>
</dependency>
```

*ddth-dao-jdbc*: include all *ddth-dao-core* and Spring-Jdbc dependencies.

```xml
<dependency>
    <groupId>com.github.ddth</groupId>
    <artifactId>ddth-dao-jdbc</artifactId>
    <version>0.8.0.4</version>
    <type>pom</type>
</dependency>
```

*ddth-dao-cassandra*: include all *ddth-dao-core* and Cassandra dependencies.

```xml
<dependency>
    <groupId>com.github.ddth</groupId>
    <artifactId>ddth-dao-cassandra</artifactId>
    <version>0.8.0.4</version>
    <type>pom</type>
</dependency>
```

## Usage

Assuming you are using Jdbc-based DAO.

### Implement your BO class

Sample BO class:

```java
import java.util.Date;
import com.github.ddth.dao.BaseBo;

public class MyBo extends BaseBo {
    private final static String ATTR_ID = "id";
    private final static String ATTR_NAME = "name";
    private final static String ATTR_TIMESTAMP = "timestamp";
    
    public long getId() {
        Long id = getAttribute(ATTR_ID, Long.class);
        return id != null ? id.longValue() : 0;
    }
    
    public MyBo setId(long id) {
        setAttribute(ATTR_ID, id);
        return this;
    }

    public String getName() {
        return getAttribute(ATTR_NAME, String.class);
    }
    
    public MyBo setName(String name) {
        return (MyBo) setAttribute(ATTR_NAME, name);
    }
    
    public String getTimestamp() {
        return getAttribute(ATTR_TIMESTAMP, Date.class);
    }
    
    public MyBo setTimestamp(Date timestamp) {
        return (MyBo) setAttribute(ATTR_TIMESTAMP, timestamp!=null ? timestamp : new Date());
    }
}
```

### Implement interface IRowMapper to transform a ResultSet into your BO

Sample RowMapper class:

```java
import java.sql.ResultSet;
import java.sql.SQLException;
import com.github.ddth.dao.jdbc.IRowMapper

public class MyBoMapper implements IRowMapper<MyBo> {
    public final static String TABLE_COL_ID = "column_id";
    public final static String TABLE_COL_NAME = "column_name";
    public final static String TABLE_COL_TIMESTAMP = "column_timestamp";

    @Override
    public MyBo mapRow(ResultSet rs, int rowNum) throws SQLException {
        MyBo bo = new MyBo();
        bo.setId(rs.getInt(TABLE_COL_ID));
        bo.setName(rs.getString(TABLE_COL_NAME));
        bo.setTimestamp(rs.getTimestamp(TABLE_COL_TIMESTAMP));
        bo.markClean();
        return bo;
    }
}
```

### Implement your Jdbc-based DAO class

Sample Jdbc DAO class:

```java
import java.sql.SQLException;
import org.apache.commons.lang3.StringUtils;
import com.github.ddth.dao.jdbc.BaseJdbcDao;

public class MyJdbcDao extends BaseJdbcDao {
    /* -= Fetch a record from db =- */

    private final static String[] COL_SELECT = {
            MyBoMapper.TABLE_COL_ID, 
            MyBoMapper.TABLE_COL_NAME,
            MyBoMapper.TABLE_COL_TIMESTAMP
        };
    private final static String SQL_SELECT = "SELECT " + StringUtils.join(COL_SELECT, ',')
        + " FROM table_name WHERE " + MyBoMapper.COL_ID + "=?"; 

    public MyBo get(long id) {
        final String cacheKey = String.valueOf(id);
        final String cacheName = "CACHE_BO";
        MyBo result = getFromCache(cacheName, cacheKey, MyBo.class);
        if (result == null) {
            final Object[] WHERE_VALUES = new Object[] { id };
            try {
                List<MyBo> dbRows = executeSelect(MyBoMapper.instance, SQL_SELECT, WHERE_VALUES);
                result = dbRows != null && dbRows.size() > 0 ? dbRows.get(0) : null;
                putToCache(cacheName, cacheKey, result);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        return result;
    }
    
    /* -= Delete a record from db =- */
    
    private final static String SQL_DELETE = "SELECT FROM table_name WHERE " + MyBoMapper.COL_ID + "=?"; 
    
    public boolean delete(MyBo bo) {
        final String cacheKey = String.valueOf(bo.getId());
        final String cacheName = "CACHE_BO";
        final Object[] VALUES = new Object[] { bo.getId() };
        try {
            int numRows = execute(SQL_DELETE, VALUES);
            removeFromCache(cacheName, cacheKey);
            return numRows > 0;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    
    /* -= Update an existing record =- */
    
    private String SQL_UPDATE = "UPDATE table_name SET "
        + StringUtils.join(new String[] {
            MyBoMapper.COL_NAME + "=?",
            MyBoMapper.COL_TIMESTAMP + "=?"
        }, ',') + " WHERE " + MyBoMapper.COL_ID + "=?";
    
    public boolean update(MyBo bo) {
        final String cacheKey = String.valueOf(bo.getId());
        final String cacheName = "CACHE_BO";
        final Object[] VALUES = new Object[] { bo.getName(), bo.getTimestamp(), bo.getId() };
        try {
            int numRows = execute(SQL_UPDATE, VALUES);
            removeFromCache(cacheName, cacheKey);
            return numRows > 0;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    
    /* -= Create a new record, assuming <id> is auto-increased number =- */
    
    private final static String[] COL_CREATE = {
            MyBoMapper.TABLE_COL_NAME,
            MyBoMapper.TABLE_COL_TIMESTAMP
        };

    private String SQL_CREATE = "INSERT INTO table_name ("
            + StringUtils.join(COL_CREATE, ',') + ") VALUES ("
            + StringUtils.repeat("?", ",", COL_CREATE.length) + ")";
    
    public boolean create(MyBo bo) {
        final Object[] VALUES = new Object[] { bo.getName(), bo.getTimestamp() };
        try {
            int numRows = execute(SQL_CREATE, VALUES);
            return numRows > 0;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
```

### Use your DAO class

```java
import com.github.ddth.dao.jdbc.jdbctemplate.JdbcTemplateJdbcHelper;
...
// (required) create a IJdbcHelper instance
DataSource ds = createJdbcDataSource();

IJdbcHelper jdbcHelper = new JdbcTemplateJdbcHelper()
jdbcHelper.setDataSource(ds);
jdbcHelper.init();

// (optional) create a cache factory to improve DAO performance
// see https://github.com/DDTH/ddth-cache-adapter
ICacheFactory cacheFactory = createCacheFactory();

MyJdbcDao dao = MyJdbcDao();
dao.setJdbcHelper(jdbcHelper);     // required
dao.setCacheFactory(cacheFactory); // optional
dao.init(); //initialize the DAO

// create a new record
MyBo bo = new MyBo();
bo.setId(1).setName("btnguyen2k").setTimestamp(new Date());
dao.create(bo);

// fetch an existing record
MyBo another = dao.get(1);

// update an existing record
bo.setName("another name");
dao.update(bp);

// delete an existing record
dao.delete(bo);

dao.destroy(); //clean-up
```
