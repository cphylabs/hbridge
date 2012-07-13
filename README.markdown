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
libraryDependencies ++= Seq("com.cloudphysics" % "hbridge" % "1.1.0")
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
      <version>1.1.0</version>
    </dependency>
 </dependencies>
```

## Quickstart


This library is composed of 2 simple abstractions :

# HBridgeConfig 
	* This class is a clean abstraction of Hadoop Configuration class (including and not limited to ZKQuorum, ZKPort, HMaster , etc )
	* All properties pertaining to HBase client access can be composed in a externalized YAML properties file and loaded during client startup
    Example :
		val zkQuorum = config.getString("hbaseZookeeperQuorum").get
		val zkClientPort = config.getString("hbaseZookeeperClientPort").get
		val hbaseWriteBuffer = config.getString("hbaseWriteBufferSize").get
		val hbaseMaster = config.getString("hbaseMaster").get
		val hbridgeConfig = HBridgeConfig(zkQuorum, zkClientPort, hbaseWriteBuffer, hbaseMaster)
	
# HBridge
  	* This class is a clean abstraction over HTable (both in DDL and DML perspective)
	* After successfully acquiring HBridgeConfig
	
		def withHbase(hbridgeConfig: HBridgeConfig,
			rowKey: String,
			tableName: String,
			cfName: String,
			cqName: String,
			cqinventoryPoolSize: Int,
			chunkSize: Int)(f: (HBridge, String, String, String) => Unit) {
				val hbridge = HBridge(hbridgeConfig, tableName)
				hbridge.setAutoFlush(false)
				try {
				f(hbridge, rowKey, cfName, cqName)
				} finally {
				hbridge.commit // Add a COMMIT only for write operations and NOT for READ (FETCH) operations
				hbridge.returnToPool
				log.info("Returned to Pool")
				-- optionally tear down when done (Preferably don't close the pool in web application server that run in
				-- continuous mode 
				hbridge.closeTablePool(tableName)
				}
		}

		--- Now use this function as illustrated below 
		withHbase(hbridgeConfig, rowKey, historyTableName, historyCF, fileQualifier, inventoryPoolSize, chunkSize) {
			(hbridge, rowKey, cf, cq) =>
			hbridge.put(rowKey, cf, cq, value)
			hbridge.scanRow ....
		}


# License
  -------

  Published under The MIT License