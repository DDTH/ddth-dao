# Build application's DAO from ddth-dao-core

## Maven dependencies

```xml
<dependency>
    <groupId>com.github.ddth</groupId>
    <artifactId>ddth-dao-core</artifactId>
    <version>${ddth-dao-version}</version>
</dependency>
```

## Implement Application's BO classes

`ddth-dao-core` provides 3 base classes to implement application's business objects.

`BaseBo` is the starting point to implement application's BO:

```java
import java.util.Date;
import com.github.ddth.dao.BaseBo;

public class MyBo extends BaseBo {
    /**
     * Helper method to create new {@link MyBo} objects.
     * @param id
     * @return 
     */
    public static MyBo newInstance(long id) {
        MyBo bo = new MyBo();
        bo.setId(id);
        return bo;
    }
    
    private final static String ATTR_ID = "id";
    private final static String ATTR_NAME = "name";
    private final static String ATTR_TIMESTAMP = "timestamp";
    
    public long getId() {
        Long id = getAttribute(ATTR_ID, Long.class);
        return id != null ? id.longValue() : 0;
    }
    
    public void setId(long id) {
        setAttribute(ATTR_ID, id);
    }

    public String getName() {
        return getAttribute(ATTR_NAME, String.class);
    }
    
    public void setName(String name) {
        setAttribute(ATTR_NAME, name!=null ? name.trim() : null);
    }
    
    public Date getTimestamp() {
        return getAttribute(ATTR_TIMESTAMP, Date.class);
    }
    
    public void setTimestamp(Date timestamp) {
        setAttribute(ATTR_TIMESTAMP, timestamp!=null ? timestamp : new Date());
    }
}

MyBo bo = MyBo.newInstance(1);
bo.setName("btnguyen2k");
bo.setTimestamp(new Date());
System.out.println("ID: " + bo.getId());

// work with arbitrary attributes
bo.setAttribute("age", 123);
System.out.println(bo.getAttributeOptional("age", Integer.class).orElse(0));
bo.setAttribute("msg", "Hello, world!");
System.out.println(bo.getAttribute("msg", String.class));

// the following line throws NullPointerException if attribute "age" does not exist,
// or can not be converted to integer
int age = bo.getAttribute("age", int.class);

// the following line behaves pretty much the same
int age = bo.getAttribute("age", Integer.class);

// it's safer to use BaseBo.getAttributeOptional when working with primitives
boolean isChecked = bo.getAttributeOptional("checked", Boolean.class).orElse(false);
int age = bo.getAttributeOptional("age", Integer.class).orElse(0);

// this prints: {"msg":"Hello, world!","name":"btnguyen2k","id":1,"age":123,"timestamp":1541328781376}
System.out.println(bo.getAttributesAsJsonString());
```

If application works with JSON, there are `BaseJsonBo` and `BaseDataJsonFieldBo`.
`BaseJsonBo` extends `BaseBo`: it's pretty much like `BaseBo`, except that each attribute is JSON-encoded.

```java
public class MyBo extends BaseJsonBo {
    //...
}

MyBo bo = MyBo.newInstance(1);
bo.setName("btnguyen2k");
bo.setTimestamp(new Date());
System.out.println("ID: " + bo.getId());

// work with arbitrary attributes
bo.setAttribute("age", 123);
System.out.println(bo.getAttributeOptional("age", Integer.class).orElse(0));
bo.setAttribute("msg", "Hello, world!");
System.out.println(bo.getAttribute("msg", String.class));

// this prints: {"msg":"\"Hello, world!\"","name":"\"btnguyen2k\"","id":"1","age":"123","timestamp":"1541328857895"}
System.out.println(bo.getAttributesAsJsonString());
```

Note the difference in outputs of `BaseBo.getAttributesAsJsonString()` and `BaseJsonBo.getAttributesAsJsonString()`?
Also, unlike `BaseBo.getAttribute()` that returns an `Object`, `BaseJsonBo.getAttribute(...)` returns a `JsonNode`!

```java
// If an attribute is a map or list, sub-attributes are accessed using d-path

MyBo org = MyBo.newInstance(1);
org.setAttribute("name", "DDTH");
org.setAttribute("year", 2018);
org.setSubAttr("founder", "name", "Thanh Nguyen");
org.setSubAttr("founder", "email", "btnguyen2k(at)gmail.com");

org.setSubAttr("addr", "number", 123);
org.setSubAttr("addr", "street", "Abc");
org.setSubAttr("addr", "surburb", "X");

// note: developers.[0] and developers[0] are both valid
org.setSubAttr("employees", "developers.[0].name", "Thanh Nguyen");
org.setSubAttr("employees", "developers[0].email", "btnguyen2k(at)gmail.com");
org.setSubAttr("employees", "developers.[1].name", "Thanh Ba Nguyen");
org.setSubAttr("employees", "developers[1].email", "btnguyen2k(at)yahoo.com");
org.setSubAttr("employees", "admin.name.first", "Thanh");
org.setSubAttr("employees", "admin.name.last", "Nguyen");

Map<String, Object> attrs = org.getAttributes();

// this prints: "DDTH"
// note the double quotes as "DDTH" is JSON-encoded of string DDTH
System.out.println(attrs.get("name"));

// this prints: 2018
System.out.println(attrs.get("year"));

// this prints: {"name":"Thanh Nguyen","email":"btnguyen2k(at)gmail.com"}
System.out.println(attrs.get("founder"));
// this print: "Thanh Nguyen"
System.out.println(org.getSubAttr("founder", "name"));
//this print: "btnguyen2k(at)gmail.com"
System.out.println(org.getSubAttr("founder", "email"));


// this prints: [{"name":"Thanh Nguyen","email":"btnguyen2k(at)gmail.com"},{"name":"Thanh Ba Nguyen","email":"btnguyen2k(at)yahoo.com"}]
System.out.println(org.getSubAttr("employees", "developers"));
// this prints: {"name":"Thanh Nguyen","email":"btnguyen2k(at)gmail.com"}
System.out.println(org.getSubAttr("employees", "developers[0]"));
// this prints: "Thanh Ba Nguyen"
System.out.println(org.getSubAttr("employees", "developers.[1].name"));
// this prints: "Thanh"
System.out.println(org.getSubAttr("employees", "admin.name.first"));

// sub-attributes can be removed
org.removeSubAttr("name", "last");
org.removeSubAttr("addr", "surburb");
```

`BaseDataJsonFieldBo` also extends `BaseBo`, but it has a special field `data` which is JSON-encoded.
Other fields behave the same as `BaseBo`'s fields.

```java
public class MyBo extends BaseDataJsonFieldBo {
    ...
}

// BaseDataJsonFieldBo can be used just like a BaseBo
MyBo org = MyBo.newInstance(1);
org.setName("DDTH");
org.setTimestamp(new Date());

// this prints: ID: 1
System.out.println("ID: " + org.getId());

// however, BaseDataJsonFieldBo has a special "data" field which is a JSON.
org.setDataAttr("year", 2018);
// this prints: 2018
System.out.println(org.getDataAttrOptional("year", Integer.class).orElse(0));

org.setDataAttr("msg", "Hello, world!");
// this prints: "Hello, world!"
System.out.println(org.getDataAttr("msg", String.class));

// this prints: {"data":"{\"year\":2018,\"msg\":\"Hello, world!\"}","name":"DDTH","id":1,"timestamp":1541399057539}
System.out.println(org.getAttributesAsJsonString());

// "data" is a JSON, its usage is similar to a field of BaseJsonBo
org.setDataAttr("founder.name", "Thanh Nguyen");
org.setDataAttr("founder.email", "btnguyen2k(at)gmail.com");

org.setDataAttr("addr.number", 123);
org.setDataAttr("addr.street", "Abc");
org.setDataAttr("addr.surburb", "X");

org.setDataAttr("employees.developers.[0].name", "Thanh Nguyen");
org.setDataAttr("employees.developers[0].email", "btnguyen2k(at)gmail.com");
org.setDataAttr("employees.developers.[1].name", "Thanh Ba Nguyen");
org.setDataAttr("employees.developers[1].email", "btnguyen2k(at)yahoo.com");
org.setDataAttr("employees.admin.name.first", "Thanh");
org.setDataAttr("employees.admin.name.last", "Nguyen");

JsonNode dataAttrs = org.getDataAttrs();
// this prints: 2018
System.out.println(dataAttrs.get("year"));
// this prints: "Hello, world!"
System.out.println(dataAttrs.get("msg"));

// this prints: {"name":"Thanh Nguyen","email":"btnguyen2k(at)gmail.com"}
System.out.println(dataAttrs.get("founder"));
// this prints: "Thanh Nguyen"
System.out.println(org.getDataAttr("founder.name"));
// this prints: "btnguyen2k(at)gmail.com"
System.out.println(org.getDataAttr("founder.email"));

// this prints: [{"name":"Thanh Nguyen","email":"btnguyen2k(at)gmail.com"},{"name":"Thanh Ba Nguyen","email":"btnguyen2k(at)yahoo.com"}]
System.out.println(org.getDataAttr("employees.developers"));
// this prints: {"name":"Thanh Nguyen","email":"btnguyen2k(at)gmail.com"}
System.out.println(org.getDataAttr("employees.developers[0]"));
// this prints: "Thanh Ba Nguyen"
System.out.println(org.getDataAttr("employees.developers.[1].name"));
// this prints: "Thanh"
System.out.println(org.getDataAttr("employees.admin.name.first"));

// "data"'s sub-attributes can be removed
org.removeDataAttr("year");
org.removeDataAttr("addr.surburb");
```

## Implement Application's DAO classes

`ddth-dao-core` provides some low-level abstract classes and interfaces as a foundation to implement DAO classes:

- `BaseDao`: starting point to implement DAO. It also contains foundation methods to put/get items to/from cache.
- `IGenericBoDao`: API interface to implement DAO that manages one single BO class.
- `IGenericMultiBoDao`: API interface to implement DAO that manages multi-BO classes.

However, there are also ready-to-use classes to implement [JDBC-based](JDBC.md) DAO or [NoSQL-based](NOSQL.md) DAO.

See:
- Implement JDBC-based DAO: [JDBC.md](JDBC.md).
- Implement NoSQL-based DAO: [NOSQL.md](NOSQL.md).
