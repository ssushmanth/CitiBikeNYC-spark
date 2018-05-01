
name := """citi-bike-nyc-spark-project"""

EclipseKeys.withSource := true

version := "1.0"

scalaVersion := "2.11.2"


ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

jarName in assembly := "citi-bike-nyc-spark-project.jar"

assemblyMergeStrategy in assembly := {
  case "application.conf" => MergeStrategy.concat
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "1.5.2",
  "org.apache.spark" % "spark-sql_2.11" % "1.5.2",
  "org.apache.spark" % "spark-mllib_2.11" % "1.5.2",
  "databricks" % "spark-csv" % "1.5.0-s_2.11",
  "org.slf4j" % "slf4j-log4j12" % "1.7.10"
)


resolvers ++= Seq(
  "databricks repository" at "https://dl.bintray.com/spark-packages/maven/",
  "artifactory" at "http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases"
)


scalacOptions ++= Seq(
  "-unchecked",
  "-deprecation",
  "-Xlint",
  "-Ywarn-dead-code",
  "-language:_",
  "-target:jvm-1.7",
  "-encoding", "UTF-8"
)

javaOptions in run += "-Xmx8G"


parallelExecution in Test := false

testOptions += Tests.Argument(TestFrameworks.JUnit, "-v")
