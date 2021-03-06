[![Build Status](https://travis-ci.org/DDTH/ddth-dao.svg?branch=master)](https://travis-ci.org/DDTH/ddth-dao) [![Javadocs](http://javadoc.io/badge/com.github.ddth/ddth-dao-core.svg)](http://javadoc.io/doc/com.github.ddth/ddth-dao-core)

# ddth-dao

DDTH's DAO library: utility and base classes to implement application's data access layer.

By Thanh Ba Nguyen (btnguyen2k (at) gmail.com).

Project home:
[https://github.com/DDTH/ddth-dao](https://github.com/DDTH/ddth-dao)

**`ddth-dao` requires Java 11+ since v1.0.0, for Java 8, use v0.10.y**

## Installation

Latest release version: `1.1.1`. See [RELEASE-NOTES.md](RELEASE-NOTES.md).

Maven dependency: if only a sub-set of `ddth-dao` functionality is used, choose the corresponding
dependency artifact(s) to reduce the number of unused jar files.

*ddth-dao-core*: core classes for DAO pattern, all other dependencies are *optional*.

```xml
<dependency>
    <groupId>com.github.ddth</groupId>
    <artifactId>ddth-dao-core</artifactId>
    <version>1.1.1</version>
</dependency>
```

*ddth-dao-cassandra*: include all *ddth-dao-core* and Cassandra dependencies.

```xml
<dependency>
    <groupId>com.github.ddth</groupId>
    <artifactId>ddth-dao-cassandra</artifactId>
    <version>1.1.1</version>
    <type>pom</type>
</dependency>
```

*ddth-dao-jdbc*: include all *ddth-dao-core* and Spring-Jdbc dependencies.

```xml
<dependency>
    <groupId>com.github.ddth</groupId>
    <artifactId>ddth-dao-jdbc</artifactId>
    <version>1.1.1</version>
    <type>pom</type>
</dependency>
```

*ddth-dao-lucenee*: include all *ddth-dao-core* and Lucene dependencies.

```xml
<dependency>
    <groupId>com.github.ddth</groupId>
    <artifactId>ddth-dao-lucene</artifactId>
    <version>1.1.1</version>
    <type>pom</type>
</dependency>
```

## Usage

- Build application's DAO from [ddth-dao-core](CORE.md).
- Implement [JDBC-based](JDBC.md) DAO or [NoSQL](NOSQL.md) DAO.


## License

See LICENSE.txt for details. Copyright (c) 2014-2019 Thanh Ba Nguyen.

Third party libraries are distributed under their own license(s).
