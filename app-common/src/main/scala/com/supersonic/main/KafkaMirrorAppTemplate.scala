package com.supersonic.main

import akka.actor.ActorSystem
import akka.event.{Logging, LoggingAdapter}
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.FlowMonitorState.{Failed, Finished, Initialized, Received}
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{ActorMaterializer, FlowMonitor, Materializer}
import com.supersonic.BuildInfo
import com.supersonic.kafka_mirror.MirrorCommand.VerifyState
import com.supersonic.kafka_mirror._
import com.supersonic.main.KafkaMirrorAppTemplate._
import com.supersonic.util.ActorSystemUtil._
import com.supersonic.util.SignalingUtil
import com.typesafe.config.ConfigFactory
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ValueReader
import shapeless.Typeable
import spray.json.JsonFormat
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

/** An abstract skeleton for apps that run a Kafka mirroring service.
  * This includes a healthcheck server with info about the app as well as the mirroring process itself.
  *
  * Implementors of this class need to provide a backend source for mirror configuration to run this
  * app.
  */
trait KafkaMirrorAppTemplate {
  /** The value that is materialized by the source of mirror configuration for the app. */
  protected type MirrorConfigSourceMat

  /** The application setting to be used by the application. */
  protected type AppSettings <: BaseAppSettings

  protected implicit def appSettingsValueReader: ValueReader[AppSettings]

  protected implicit def appSettingsJSONFormat: JsonFormat[AppSettings]

  /** Needed for the derivation of the AppState JSON format */
  protected implicit def appSettingsTypeable: Typeable[AppSettings]

  /** Will be used to render the current state in an HTTP-route. */
  protected implicit val appStateFormat = AppState.format[AppSettings]

  /** A callback to be invoked when the application completes. */
  protected def onCompletion(matValue: MirrorConfigSourceMat): Unit

  /** An optional custom route to be served by the application instead of [[defaultRoute]].
    * The given callback give access to the current state of the application.
    */
  protected def customRoute(currentState: () => AppState[AppSettings]): Option[Route]

  /** The type of sources that provide mirror configuration for this app. */
  protected type MirrorConfigSource = Source[Map[MirrorID, Option[String]], MirrorConfigSourceMat]

  /** Given the settings for the application, creates a source of mirror configuration and invoke
    * the given continuation with it.
    *
    * @param continuation A continuation that must be invoked when by the implementors of this class.
    */
  protected def withConfigSource[A](appSettings: AppSettings)
                                   (continuation: MirrorConfigSource => A)
                                   (implicit logger: LoggingAdapter,
                                    executionContext: ExecutionContext): A

  def main(args: Array[String]): Unit = withActorSystem("KafkaMirrorApp") { implicit system =>
    implicit val logger: LoggingAdapter = Logging(system.eventStream, "KafkaMirrorApp")
    implicit val materializer: Materializer = ActorMaterializer()
    implicit val executionContext: ExecutionContext = system.dispatcher

    val appSettings = ConfigFactory.load().as[AppSettings]("kafka-mirror-settings")

    withConfigSource(appSettings) { mirrorConfigSource =>
      val mirrorMaker = new SourceBackedMirrorMaker(materializer, logger)
      val mirrorManager = new MirrorManager(mirrorMaker)

      val stateVerificationInterval = appSettings.stateVerificationInterval.getOrElse(stateVerificationIntervalDefault)

      val flow = mirrorConfigSource
        .via(SettingsProcessingFlow(system.settings.config))
        .merge(Source.tick(stateVerificationInterval, stateVerificationInterval, VerifyState))
        .mergeMat(Source.maybe[MirrorCommand.Shutdown.type])(Keep.both)
        .via(mirrorManager.flow)
        .monitor()((mat, monitor) => (mat, monitor))

      val (((mirrorConfigSourceMat, shutdownHandle), monitor), endOfWorld) = flow.toMat(Sink.ignore)(Keep.both).run()

      val serverBinding = initRoute(appSettings, monitor)

      def shutdown() = {
        onCompletion(mirrorConfigSourceMat)
        shutdownHandle.trySuccess(Some(MirrorCommand.Shutdown))
        val _ = Await.ready(serverBinding.unbind(), serverShutdownTimeout)
      }

      endOfWorld.onComplete { _ =>
        onCompletion(mirrorConfigSourceMat)
      }

      SignalingUtil.registerHandler(shutdown)

      val _ = Await.result(endOfWorld, Duration.Inf)
    }
  }

  private def initRoute(appSettings: AppSettings,
                        monitor: FlowMonitor[Map[MirrorID, RunningMirror]])
                       (implicit logger: LoggingAdapter,
                        actorSystem: ActorSystem,
                        materializer: Materializer,
                        executionContext: ExecutionContext): Http.ServerBinding = {
    val currentState = () => { //TODO add mirrors that are present in the config but not properly configured
      val currentMirrors = monitor.state match {
        case Received(mirrors) => Right(mirrors.mapValues(_.mirrorSettings).toList)
        case Initialized => Left("Initialized Kafka mirrors stream")
        case Failed(t) => Left(s"Failed while running Kafka mirrors stream: ${t.getMessage}")
        case Finished => Left(s"Finished running Kafka mirrors stream")
      }

      AppState(
        appVersion = BuildInfo.gitDescribedVersion,
        commitHash = BuildInfo.gitCommit,
        settings = appSettings,
        mirrors = currentMirrors)
    }

    val route = customRoute(currentState).getOrElse(defaultRoute(currentState))

    val eventualServerBinding = Http().bindAndHandle(route, "0.0.0.0", appSettings.port)

    val serverBinding = Await.result(eventualServerBinding, Duration.Inf)
    logger.info(s"Server: ${serverBinding.localAddress} started successfully")

    serverBinding
  }

  private def defaultRoute(currentState: () => AppState[AppSettings]): Route =
    path("healthcheck") {
      get {
        complete(currentState())
      }
    }
}

object KafkaMirrorAppTemplate {
  trait BaseAppSettings {
    def port: Int

    def stateVerificationInterval: Option[FiniteDuration]
  }

  val stateVerificationIntervalDefault = 5.seconds
  val serverShutdownTimeout = 5.seconds
}
