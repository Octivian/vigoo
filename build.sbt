name := "vigoo"

version := "1.0"

scalaVersion := "2.10.4"

val sparkVersion = "1.3.0-cdh5.4.0"

val hbaseVersion = "1.0.0-cdh5.4.0"

libraryDependencies <<= scalaVersion {
  scala_version => Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,

    "org.mongodb" %% "casbah" % "2.7.3",

    "org.scalaj" %% "scalaj-collection" % "1.6",

    "net.liftweb" %% "lift-json" % "2.6",

    "org.apache.hbase" % "hbase-server" % hbaseVersion,

    "org.apache.hbase" % "hbase-common" % hbaseVersion,

    "org.apache.hbase" % "hbase-client" % hbaseVersion
  )
}



resolvers += "Sonatype OSS Releases" at "https://oss.sonatype.org/content/repositories/releases/"

resolvers += "BFil Nexus Releases" at "http://nexus.b-fil.com/nexus/content/repositories/releases/"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

resolvers += "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/"


    