package com.github.mnogu.gatling.kafka.action

import com.github.mnogu.gatling.kafka.protocol.KafkaProtocol
import com.github.mnogu.gatling.kafka.request.builder.{KafkaAttributes, KafkaAvroAttributes}
import com.sksamuel.avro4s.{AvroInputStream, Decoder, Encoder, Record, RecordFormat}
import io.gatling.commons.stats.{KO, OK}
import io.gatling.commons.util.DefaultClock
import io.gatling.commons.validation.Validation
import io.gatling.core.CoreComponents
import io.gatling.core.action.{Action, ExitableAction}
import io.gatling.core.session._
import io.gatling.core.util.NameGen
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer._
import com.sksamuel.avro4s.RecordFormat._

import scala.collection.Set
import scala.collection.immutable.Set
import scala.util.Success



class KafkaRequestAvroAction[K, V](val producer: KafkaProducer[K, GenericRecord],
                                   val kafkaAttributes: KafkaAvroAttributes[K, V],
                                   val coreComponents: CoreComponents,
                                   val kafkaProtocol: KafkaProtocol,
                                   val throttled: Boolean,
                                   val next: Action)
  extends ExitableAction with NameGen {

  implicit val format: RecordFormat[V] = kafkaAttributes.recordFormat
  implicit val decoder: Decoder[V] = kafkaAttributes.decoder

  val statsEngine = coreComponents.statsEngine
  val clock = new DefaultClock
  override val name = genName("kafkaRequest")

  override def execute(session: Session): Unit = recover(session) {

    kafkaAttributes requestName session flatMap { requestName =>

      val outcome =
        sendRequest(
          requestName,
          producer,
          kafkaAttributes,
          throttled,
          session)

      outcome.onFailure(
        errorMessage =>
          statsEngine.reportUnbuildableRequest(session, requestName, errorMessage)
      )

      outcome

    }

  }

  private def sendRequest(requestName: String,
                          producer: Producer[K, GenericRecord],
                          kafkaAttributes: KafkaAvroAttributes[K, V],
                          throttled: Boolean,
                          session: Session): Validation[Unit] = {

    kafkaAttributes payload session map { payload =>


      val input = AvroInputStream.json[V].from(payload.getBytes("UTF-8")).build
      val payloadParsed: V = input.iterator.toList.head
      input.close()

      val recordValue: GenericRecord = format.to(payloadParsed)
      val record = kafkaAttributes.key match {
        case Some(k) =>
          new ProducerRecord[K, GenericRecord](kafkaProtocol.topic, k(session).toOption.get, recordValue)
        case None =>
          new ProducerRecord[K, GenericRecord](kafkaProtocol.topic, recordValue)
      }

      val requestStartDate = clock.nowMillis

      producer.send (record, (m: RecordMetadata, e: Exception) => {

        val requestEndDate = clock.nowMillis
        statsEngine.logResponse(
          session,
          requestName,
          startTimestamp = requestStartDate,
          endTimestamp = requestEndDate,
          if (e == null) OK else KO,
          None,
          if (e == null) None else Some(e.getMessage)
        )

        if (throttled) {
          coreComponents.throttler.throttle(session.scenario, () => next ! session)
        } else {
          next ! session
        }

      })

    }

  }

}
