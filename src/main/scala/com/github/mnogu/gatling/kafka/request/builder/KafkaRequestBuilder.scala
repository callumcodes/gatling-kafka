package com.github.mnogu.gatling.kafka.request.builder

import com.github.mnogu.gatling.kafka.action.{KafkaRequestActionBuilder, KafkaRequestAvroActionBuilder}
import com.sksamuel.avro4s.{Decoder, RecordFormat}
import io.gatling.core.session._
import org.apache.avro.Schema

case class KafkaAttributes[K, V](requestName: Expression[String],
                                 key: Option[Expression[K]],
                                 payload: Expression[V])

case class KafkaAvroAttributes[K, V](requestName: Expression[String],
                                     key: Option[Expression[K]],
                                     payload: Expression[String],
                                     decoder: Decoder[V],
                                     recordFormat: RecordFormat[V],
                                     schema: Schema)

case class KafkaRequestBuilder(requestName: Expression[String]) {

  def send[V](payload: Expression[V]): KafkaRequestActionBuilder[_, V] = send(payload, None)

  def send[K, V](key: Expression[K], payload: Expression[V]): KafkaRequestActionBuilder[K, V] = send(payload, Some(key))

  def sendAvro[V](payload: Expression[String])(implicit format: RecordFormat[V], decoder: Decoder[V], schema: Schema): KafkaRequestAvroActionBuilder[_, V] = sendAvro(payload, None)

  def sendAvro[K, V](key: Expression[K], payload: Expression[String])(implicit format: RecordFormat[V], decoder: Decoder[V], schema: Schema): KafkaRequestAvroActionBuilder[K, V] = sendAvro[K, V](payload, Some(key))

  private def send[K, V](payload: Expression[V], key: Option[Expression[K]]) =
    new KafkaRequestActionBuilder(KafkaAttributes(requestName, key, payload))

  private def sendAvro[K, V](payload: Expression[String], key: Option[Expression[K]])(implicit format: RecordFormat[V], decoder: Decoder[V], schema: Schema) =
    new KafkaRequestAvroActionBuilder[K, V](KafkaAvroAttributes[K, V](requestName, key, payload, decoder, format, schema))


}
