
name := "Spark"

version := "1.0"

scalaVersion := "2.11.8"

mainClass in Compile := Some("PageRank")


val overrideScalaVersion = "2.11.8"
val sparkVersion = "1.5.0"
val sparkXMLVersion = "0.3.3"
val sparkCsvVersion = "1.4.0"
val sparkElasticVersion = "2.3.4"
val sscKafkaVersion = "1.6.2"
val sparkMongoVersion = "1.0.0"
val sparkCassandraVersion = "1.6.0"
//Override Scala Version to the above 2.11.8 version

ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }
resolvers ++= Seq(
  "All Spark Repository -> bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"
)
libraryDependencies ++= Seq(
  "org.apache.spark"      %%  "spark-core"      %   sparkVersion  exclude("jline", "2.12")
)
// META-INF discarding
mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
{
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
}