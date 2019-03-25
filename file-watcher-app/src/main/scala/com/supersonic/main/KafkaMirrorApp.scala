package com.supersonic.main

import akka.NotUsed
import akka.event.LoggingAdapter
import akka.http.scaladsl.server.Route
import com.supersonic.file_watch.{ConfigFileSettingsProcessing, DirectoryFilesSource}
import net.ceedubs.ficus.readers.ValueReader
import shapeless.Typeable
import spray.json.JsonFormat
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

/** Launches an application that listens to a directory for Kafka mirror definitions and manages them.
  * The application provides a healthcheck route for its current state.
  */
object KafkaMirrorApp extends KafkaFileWatcherMirrorApp {
  protected def customRoute(currentState: () => AppState[AppSettings]): Option[Route] = None
}

trait KafkaFileWatcherMirrorApp extends KafkaMirrorAppTemplate {
  protected type MirrorConfigSourceMat = NotUsed

  protected type AppSettings = DirectoryAppSettings

  protected implicit def appSettingsValueReader: ValueReader[AppSettings] = DirectoryAppSettings.valueReader

  protected implicit def appSettingsJSONFormat: JsonFormat[AppSettings] = DirectoryAppSettings.format

  protected implicit def appSettingsTypeable: Typeable[AppSettings] = DirectoryAppSettings.typeable

  protected def onCompletion(notUsed: NotUsed): Unit = ()

  protected def withConfigSource[A](appSettings: AppSettings)
                                   (continuation: MirrorConfigSource => A)
                                   (implicit logger: LoggingAdapter,
                                    executionContext: ExecutionContext): A = {

    val directory = appSettings.getMirrorDirectoryPath
    val pollInterval = appSettings.pollInterval.getOrElse(defaultPollInterval)
    val maxBufferSize = appSettings.maxBufferSize.getOrElse(defaultMaxBufferSize)

    val directoryFilesSource = new DirectoryFilesSource

    val source = directoryFilesSource(directory, pollInterval, maxBufferSize)
      .via(ConfigFileSettingsProcessing.flow)

    continuation(source)
  }

  private def defaultPollInterval = 1.second
  private def defaultMaxBufferSize = 1000
}
