package com.github.mnogu.gatling.kafka.test

import com.github.mnogu.gatling.kafka.Predef._
import com.sksamuel.avro4s.{AvroSchema, Decoder, RecordFormat, SchemaFor}
import com.typesafe.config.ConfigFactory
import io.gatling.core.Predef._
import org.apache.avro.Schema
import org.apache.kafka.clients.producer.ProducerConfig

import scala.concurrent.duration._

class AvroSimulation extends Simulation {
  def loadConfigMap(kafkaBrokers: String, kafkaSchemaRegistryUrl: String): Map[String, String] = {
    Map(
      ProducerConfig.ACKS_CONFIG -> "1",
      // list of Kafka broker hostname and port pairs
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> kafkaBrokers,

      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG ->
        "io.confluent.kafka.serializers.KafkaAvroSerializer",
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG ->
        "io.confluent.kafka.serializers.KafkaAvroSerializer",
      "schema.registry.url" -> kafkaSchemaRegistryUrl
    )
  }

  val config = ConfigFactory.load("application")
  val kafkaTopics = config.getString("kafka.topics").split(",").map(_.trim)
  val kafkaBrokers = config.getString("kafka.brokers")
  val kafkaSchemaRegistryUrl = config.getString("kafka.schema-registry-url")

  case class Composer(name: Option[String], birthplace: Option[String], compositions: Seq[String])

  /*
  Json is used as a data source so that we can consume session attributes
  Note: Avro4s requires that all JSON fields are provided, otherwise you'll get an unsupported list exception
  */

  val json = """{"name":{"string":"ennio morricone"},"birthplace":{"string":"rome"},"compositions":["ecstasy of gold"]}"""

  val recordFormat = RecordFormat[Composer]
  val schema: Schema = AvroSchema[Composer]
  val decoder = Decoder[Composer]

  val kafkaConf = kafka
    // Kafka topic name
    .topic("test")
    // Kafka producer configs
    .properties(loadConfigMap(kafkaBrokers, kafkaSchemaRegistryUrl))

  val scn = scenario("Kafka Test")
    .exec(
      kafka("request")
        .sendAvro[String, Composer]("${key}", json)(recordFormat, decoder, schema)
    )

  setUp(
    scn
      .inject(constantUsersPerSec(10) during(90.seconds)))
    .protocols(kafkaConf)
}
