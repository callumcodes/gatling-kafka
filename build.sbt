name := "gatling-kafka"

organization := "com.github.mnogu"

version := "0.3.0-SNAPSHOT"

scalaVersion := "2.12.10"


val gatlingVersion = "3.3.1"
val kafkaVersion = "2.4.0"
val avro4sVersion = "3.0.9"

libraryDependencies ++= Seq(
  "io.gatling" % "gatling-core" % gatlingVersion % "provided",
  "com.sksamuel.avro4s" %% "avro4s-core" % avro4sVersion,
  ("org.apache.kafka" % "kafka-clients" % kafkaVersion)
    // Gatling contains slf4j-api
    .exclude("org.slf4j", "slf4j-api")
)

// Gatling contains scala-library
assemblyOption in assembly := (assemblyOption in assembly).value
  .copy(includeScala = false)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}