ddth-dao
============

DDTH's DAO support.

By Thanh Ba Nguyen (btnguyen2k (at) gmail.com)

Project home:
[https://github.com/DDTH/ddth-dao](https://github.com/DDTH/ddth-dao)

OSGi Environment: ddth-dao is packaged as an OSGi bundle.


## License ##

See LICENSE.txt for details. Copyright (c) 2014-2015 Thanh Ba Nguyen.

Third party libraries are distributed under their own license(s).


## Installation #

Latest release version: `0.5.0.1`. See [RELEASE-NOTES.md](RELEASE-NOTES.md).

Maven dependency: if only a sub-set of `ddth-dao` functionality is used, choose the corresponding
dependency artifact(s) to reduce the number of unused jar files.

```xml
<dependency>
	<groupId>com.github.ddth</groupId>
	<artifactId>ddth-dao-core</artifactId>
	<version>0.5.0.1</version>
</dependency>
```

*ddth-dao-core*: in-memory cache, all other dependencies *optional*.

```xml
<dependency>
    <groupId>com.github.ddth</groupId>
    <artifactId>ddth-dao-core</artifactId>
    <version>0.5.0</version>
</dependency>
```

*ddth-dao-jdbc*: include all *ddth-dao-core* and Spring-Jdbc dependencies.

```xml
<dependency>
    <groupId>com.github.ddth</groupId>
    <artifactId>ddth-dao-jdbc</artifactId>
    <version>0.5.0</version>
    <type>pom</type>
</dependency>
```

*ddth-dao-cassandra*: include all *ddth-dao-core* and Cassandra dependencies.

```xml
<dependency>
    <groupId>com.github.ddth</groupId>
    <artifactId>ddth-dao-cassandra</artifactId>
    <version>0.5.0</version>
    <type>pom</type>
</dependency>
```

## Usage ##

//TODO
