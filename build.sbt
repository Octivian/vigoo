name := "vigoo"

version := "1.0"

scalaVersion := "2.11.7"

val sparkVersion = "1.4.0"

libraryDependencies <<= scalaVersion {
  scala_version => Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,

    "org.mongodb" %% "casbah" % "2.7.3",

    "org.scalaj" %% "scalaj-collection" % "1.6"
  )
}

resolvers += "Sonatype OSS Releases" at "https://oss.sonatype.org/content/repositories/releases/"

resolvers += "BFil Nexus Releases" at "http://nexus.b-fil.com/nexus/content/repositories/releases/"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
    