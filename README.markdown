# Welcome to HBridge!

### What is HBridge?

HBridge unlocks the rich functionality of HBase for a Scala audience. In so doing, it attempts to achieve the following goals:

* Provide a strong, clear syntax for querying and filtration
* Perform as fast as possible while maintaining idiomatic Scala client code -- the abstractions should not show up in a profiler!
* Re-articulate HBase's data structures rather than force it into an ORM-style atmosphere.
* Use Scala's type system to its advantage--the compiler should verify the integrity of the schema.
* Be a verbose DSL--minimize boilerplate code, but be human readable!

## Project Status
This project is currently actively developed and maintained.  It is used in a large production codebase in high-throughput, memory-intensive scenarios, and has many months of bug fixes under its belt.  Because it already has a great deal of code utilizing it, there will not be many breaking changes to the API.  Instead what we usually do is provide an upgraded API that sits next to the old API, then deprecate the old one.

## Installation

SBT based dependency setup
```
// Global Repository resolver
resolvers ++= Seq("CphyLabs-Repo" at "http://cphylabs.github.com/hbridge/repository")
libraryDependencies ++= Seq("com.cloudphysics" % "hbridge" % "0.0.1")
```

Clone this project and execute
```mvn install```

This project uses [Maven](http://maven.apache.org/ "Apache Maven"). To use HBridge in your own maven project, simply add it as a dependency:

```xml
 <repositories>
    <repository>
        <id>HBridge</id>
        <name>HBridge-Repo</name>
        <url>http://cphylabs.github.com/hbridge/repository</url>
        <layout>default</layout>
    </repository>
 </repositories>
 <dependencies>
    <dependency>
      <groupId>com.cloudphysics</groupId>
      <artifactId>hbridge</artifactId>
      <version>0.0.1</version>
    </dependency>
 </dependencies>
```

## Quickstart


This library is composed of 2 simple abstractions :

# HBridgeConfig
  * This class is a clean abstraction of hbase-site.xml.
  * All properties pertaining to HBase client access can be composed in a externalized YAML properties file and loaded during client startup
    Example :

    val hConfig = HBridgeConfig("hbaseZookeeperQuorum","hbaseZookeeperClientPort","hbaseWriteBufferSize","hbaseMaster","tableName","cFamily")
    example : val x = HBaseConfig("aws.ec2.asas...","2181","20971520","aws.ec2.asas..:60000","tableName","cF")

 HBridge


# License
  -------

  Published under The MIT License, see LICENSE