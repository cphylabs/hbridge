
organization := "org.cphylabs"

name := "hbridge"

version := "1.1.10-SNAPSHOT"

scalaVersion := "2.10.0"

retrieveManaged := true

// Global Repository resolver
resolvers ++= Seq(
		"Thrift-Repo" at "http://people.apache.org/~rawson/repo",
		"Sonatype Scala-Tools"  at "https://oss.sonatype.org/content/groups/scala-tools/",
		"Apache HBase" at "https://repository.apache.org/content/repositories/releases",
		"Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases",
		"Typesafe Snapshots Repository" at "http://repo.typesafe.com/typesafe/snapshots/",
		"Twitter Repository" at "http://maven.twttr.com"
	)

resolvers += "CPhy-Artifactory" at "https://cloudphysics.artifactoryonline.com/cloudphysics/releases/"

resolvers += "CPhy-Hadoop-EMR" at "https://cloudphysics.artifactoryonline.com/cloudphysics/hadoop-emr/"
	

libraryDependencies ++= Seq(
	"org.apache.hadoop" % "hadoop-client" % "2.4.0" exclude("org.slf4j", "slf4j-log4j12"),
	"org.apache.hbase" % "hbase" % "0.94.18-emr" exclude("org.slf4j", "slf4j-log4j12"),
	"com.amazonaws" % "aws-java-sdk" % "1.9.34",
	"org.clapper" % "grizzled-slf4j_2.10" % "1.0.1",
	"joda-time" % "joda-time" % "2.0",
	"org.joda" % "joda-convert" % "1.1"
	)

/* publishing */
publishMavenStyle := true

publishTo <<= version { (v: String) =>
  val cphy = "https://cloudphysics.artifactoryonline.com/cloudphysics/"
  if (v.trim.endsWith("SNAPSHOT")) Some(
    "cphy snapshots" at cphy + "oss-snapshots-local"
  )
  else Some("releases" at cphy + "oss-releases-local")
}

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

publishArtifact in Test := false

pomIncludeRepository := { _ => false }

pomExtra := (
  <developers>
    <developer>
      <id>cphy</id>
      <name>CloudPhysics Inc</name>
      <email>opensource@cloudphysics.com</email>
    </developer>
  </developers>
)

