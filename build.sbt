
organization := "com.cloudphysics.hbridge"

name := "hbridge"

version := "0.0.1"

scalaVersion := "2.9.2"

retrieveManaged := true

crossScalaVersions := Seq("2.9.2", "2.9.1", "2.9.0", "2.8.1", "2.8.0")

// Global Repository resolver
resolvers ++= Seq(
	"Thrift-Repo" at "http://people.apache.org/~rawson/repo",
	"Cloudera-jdo" at "https://repository.cloudera.com/artifactory/cdh-build",
	"ClouderaRepo" at "https://repository.cloudera.com/content/repositories/releases",
	"ClouderaRcs" at "https://repository.cloudera.com/artifactory/cdh-releases-rcs",
	"CRepos" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
	"snapshots" at "http://scala-tools.org/repo-snapshots",
	"releases"  at "http://scala-tools.org/repo-releases",
	"Codahale" at "http://conikeec.github.com/jerkson/repository/",
	"Apache HBase" at "https://repository.apache.org/content/repositories/releases",
	"Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases",
	"Typesafe Snapshots Repository" at "http://repo.typesafe.com/typesafe/snapshots/",
	"Twitter Repository" at "http://maven.twttr.com",
	"Local Ivy Repository" at "file://" + System.getProperty("user.home") + "/.iv2/local"
	)
	
libraryDependencies <++= scalaVersion { scalaVersion =>
  // Helper for dynamic version switching based on scalaVersion
  val scalatestVersion: String => String = Map(("2.8.0" -> "1.3.1.RC2"), ("2.8.1" -> "1.5.1")) getOrElse (_, "1.6.1")
  // Akka Actor Dependencies
    Seq(
    "com.typesafe.akka" % "akka-actor" % "2.0-RC2",
    "com.typesafe.akka" % "akka-remote" % "2.0-RC2",
    "com.typesafe.akka" % "akka-kernel" % "2.0-RC2"
    )
}

scalacOptions ++= Seq("-encoding", "UTF-8", "-deprecation", "-Xcheckinit")

// Hadoop - HBase dependency
libraryDependencies ++= Seq(
    "org.apache.hadoop" % "hadoop-core" % "0.20.2-cdh3u4" ,
    "org.apache.hbase" % "hbase" % "0.90.5"
)


// Test Case Dependency
libraryDependencies ++= Seq(
  	"org.specs2" %% "specs2" % "1.9" % "test",
	"junit" % "junit" % "4.7" % "test",
	"org.scala-tools.testing" % "scalacheck_2.9.1"         % "1.9"   % "test",
	"org.scala-tools.testing" %  "test-interface"     % "0.5"   % "test",
	"junit"                   %  "junit"              % "4.10"  % "test"
)

// Configuration and Logging Dependency
libraryDependencies ++= Seq(
  "org.clapper" %% "grizzled-slf4j" % "0.6.9",
  "org.yaml" % "snakeyaml" % "1.8",
  "org.slf4j" % "slf4j-log4j12" % "1.6.4"
   )	

// JSON, IO, RPC and Time Processing Dependency
libraryDependencies ++= Seq(
	"net.liftweb" % "lift-json_2.9.1" % "2.4",
	"com.github.scala-incubator.io" % "scala-io-core_2.9.1" % "0.3.0",
	"com.github.scala-incubator.io" % "scala-io-file_2.9.1" % "0.3.0",
	"joda-time" % "joda-time" % "2.0" ,
	"org.joda" % "joda-convert" % "1.1",
	"org.apache.thrift" % "thrift" % "0.2.0",
	"com.codahale" % "jerkson_2.9.1"  % "0.5.1-SNAPSHOT"
	)

// Code Indentation Dependency
seq(scalariformSettings: _*)



