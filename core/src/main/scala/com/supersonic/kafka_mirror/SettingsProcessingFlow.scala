package com.supersonic.kafka_mirror

import akka.NotUsed
import akka.event.LoggingAdapter
import akka.stream.scaladsl.Flow
import cats.data.{NonEmptyList, Validated}
import cats.syntax.apply._
import com.supersonic.kafka_mirror.MirrorCommand._
import com.supersonic.util.ValidatedUtil._
import com.typesafe.config.{Config, ConfigFactory}
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import scala.util.control.NoStackTrace
import scala.util.{Failure, Try}

object SettingsProcessingFlow {
  /** Creates a flow that processes input maps that are keyed by the mirror IDs and the values
    * are Kafka settings, and converts them into a stream of [[MirrorCommand]]s that corresponds
    * to the changes in the incoming settings.
    */
  def apply(fallback: Config)
           (implicit logger: LoggingAdapter): Flow[Map[MirrorID, Option[String]], MirrorCommand, NotUsed] = {
    val init = (Map.empty[MirrorID, ExternalKafkaMirrorSettings], List.empty[MirrorCommand])

    def error(id: MirrorID)(e: SettingsErrors) = {
      logger.error(e,
        s"Invalid Kafka mirror configuration for [${id.value}], " +
          s"the mirror will not be started and any running mirrors with this ID will be stopped")

      None
    }

    Flow[Map[MirrorID, Option[String]]]
      .map(fromRawMap(fallback))
      .map { state =>
        state.flatMap {
          case (id, maybeSettings) =>
            maybeSettings.fold( //TODO do we actually want to stop running the mirror, or maybe just revert to the old settings?
              error(id),
              s => Some(id -> s)
            )
        }
      }
      .scan(init) { case ((prevState, _), curState) =>
        val commands = computeCommands(
          prevState = prevState,
          curState = curState)

        (curState, commands)
      }
      .mapConcat { case (_, commands) => commands }
  }

  /** Computes the [[MirrorCommand]]s that correspond to the change of state of the settings. */
  def computeCommands(prevState: Map[MirrorID, ExternalKafkaMirrorSettings],
                      curState: Map[MirrorID, ExternalKafkaMirrorSettings]): List[MirrorCommand] = {

    val ids = (prevState.keys ++ curState.keys).toSet

    ids.flatMap { id =>
      (prevState.get(id), curState.get(id)) match {
        case (Some(prevSettings), Some(curSettings))
          if prevSettings == curSettings => List.empty

        case (Some(_), Some(curSettings)) if shouldStart(curSettings) =>
          List(Start(id, curSettings))

        case (Some(_), Some(curSettings)) if !shouldStart(curSettings) =>
          List(Stop(id))

        case (Some(_), None) =>
          List(Stop(id))

        case (None, Some(curSettings)) if shouldStart(curSettings) =>
          List(Start(id, curSettings))

        case (None, Some(curSettings)) if !shouldStart(curSettings) =>
          List.empty

        case (None, None) =>
          List.empty
      }
    }.toList
  }

  private def shouldStart(settings: ExternalKafkaMirrorSettings) =
    settings.mirror.enabled

  /** Converts a bunch of [[Config]] objects into [[ExternalKafkaMirrorSettings]].
    *
    * @param fallback Provides defaults for the consumer and producer [[Config]]s.
    */
  def fromConfig(fallback: Config)
                (consumer: Config,
                 producer: Config,
                 mirror: MirrorSettings): ValidOrErrors[ExternalKafkaMirrorSettings] = {

    val consumerFallback = fallback.as[ValidOrErrors[Config]]("akka.kafka.consumer")
    val producerFallback = fallback.as[ValidOrErrors[Config]]("akka.kafka.producer")

    val consumerFull = consumerFallback.map(consumer.withFallback)
    val producerFull = producerFallback.map(producer.withFallback)

    (consumerFull, producerFull).mapN { (c, p) =>
      ExternalKafkaMirrorSettings(
        ExternalKafkaSettings(
          consumer = c,
          producer = p),
        mirror)
    }
  }

  val (consumerKey, producerKey, mirrorKey) = ("consumer", "producer", "mirror")

  /** Reads settings objects from a map that is keyed by the mirror IDs. */
  def fromRawMap(fallback: Config)
                (data: Map[MirrorID, Option[String]]): Map[MirrorID, Validated[SettingsErrors, ExternalKafkaMirrorSettings]] = {

    def parseConfigData(id: String, rawData: Option[String]) = {
      def error = Failure {
        new NoSuchElementException(s"Data is missing for ID: [$id]")
      }

      Validated.fromTry {
        rawData.map(Try(_)).getOrElse(error).flatMap(str => Try(ConfigFactory.parseString(str)))
      }.toValidatedNel
    }

    val validatedMirrors = data.map { case (id, rawData) =>
      id -> {
        val validatedConfig = parseConfigData(id.value, rawData).andThen { config =>
          val consumer = config.as[ValidOrErrors[Config]](consumerKey)
          val producer = config.as[ValidOrErrors[Config]](producerKey)
          val mirror = config.as[ValidOrErrors[MirrorSettings]](mirrorKey)

          (consumer, producer, mirror).mapN(fromConfig(fallback)).flatten
        }

        validatedConfig.leftMap(new SettingsErrors(_))
      }
    }

    validatedMirrors
  }

  class SettingsErrors(errors: NonEmptyList[Throwable]) extends NoStackTrace { // no stack trace because it clutters the output
    private def errorsString(errors: NonEmptyList[Throwable]) =
      errors.toList.map("- " ++ _.getMessage).mkString("\n")

    override def getMessage = s"""
     |Failed to parse the settings.
     |The settings should be in the following form:
     |A single HOCON configuration entry under the root with the keys:
     |- '$consumerKey'
     |- '$producerKey'
     |- '$mirrorKey'
     |Each with its own valid configuration nested in it.
     |
     |The errors that were produced are:
     |${errorsString(errors)}""".stripMargin
  }
}
