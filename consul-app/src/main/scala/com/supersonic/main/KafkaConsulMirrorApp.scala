package com.supersonic.main

import akka.event.LoggingAdapter
import akka.http.scaladsl.server.Route
import com.orbitz.consul.Consul
import com.supersonic.consul.{CancellationToken, ConsulSettingsFlow, ConsulStream}
import com.supersonic.kafka_mirror._
import net.ceedubs.ficus.readers.ValueReader
import shapeless.Typeable
import spray.json.JsonFormat
import scala.concurrent.ExecutionContext

/** Launches an application that listens to Consul for Kafka mirror definitions and manages them.
  * The application provides a healthcheck route for its current state.
  */
object KafkaConsulMirrorApp extends ConsulMirrorApp {
  protected def customRoute(currentState: () => AppState[AppSettings]): Option[Route] = None
}

trait ConsulMirrorApp extends KafkaMirrorAppTemplate {
  protected type MirrorConfigSourceMat = CancellationToken

  protected type AppSettings = ConsulAppSettings

  protected implicit def appSettingsValueReader: ValueReader[AppSettings] = ConsulAppSettings.valueReader

  protected implicit def appSettingsJSONFormat: JsonFormat[AppSettings] = ConsulAppSettings.format

  protected implicit def appSettingsTypeable: Typeable[AppSettings] = ConsulAppSettings.typeable

  protected def onCompletion(cancellationToken: CancellationToken): Unit =
    cancellationToken.cancel()

  protected def withConfigSource[A](appSettings: AppSettings)
                                   (continuation: MirrorConfigSource => A)
                                   (implicit logger: LoggingAdapter,
                                    executionContext: ExecutionContext): A = {

    val consulSettings = appSettings.consulSettings

    val consul: Consul = Consul.builder()
      .withUrl(consulSettings.url)
      .withReadTimeoutMillis(0L) // no timeout
      .build()

    ConsulSettings.verifySettings(consulSettings, consul)

    val root = consulSettings.rootKey

    val source = ConsulStream.consulKeySource(root, consul)
      .via(StaggeredEventsFilterGate(consulSettings.staggerTime))
      .via(ConsulSettingsFlow(root))

    continuation(source)
  }
}
