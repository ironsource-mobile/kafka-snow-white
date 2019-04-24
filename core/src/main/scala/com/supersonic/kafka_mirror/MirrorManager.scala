package com.supersonic.kafka_mirror

import akka.event.LoggingAdapter
import akka.kafka.scaladsl.Consumer.Control
import akka.stream.scaladsl.Flow
import akka.stream.{KillSwitch, KillSwitches}
import akka.{Done, NotUsed}
import com.supersonic.kafka_mirror.MirrorCommand._
import com.supersonic.kafka_mirror.MirrorManager._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

/** Provides a flow from [[MirrorCommand]]s to a map of [[RunningMirror]]s.
  *
  * For every incoming [[MirrorCommand]] the state is updated and the necessary side effects are
  * performed.
  */
class MirrorManager(mirrorMaker: MirrorMaker)
                   (implicit executionContext: ExecutionContext,
                    logger: LoggingAdapter) {
  private val killSwitch = KillSwitches.shared("kill-switch")

  val flow: Flow[MirrorCommand, Map[MirrorID, RunningMirror], NotUsed] = {
    val init = Map.empty[MirrorID, RunningMirror]

    Flow[MirrorCommand].scanAsync(init)(executeMirrors(mirrorMaker, killSwitch))
      .via(killSwitch.flow)
  }
}

private[kafka_mirror] object MirrorManager {
  /** The next step that should executed on the Kafka mirrors state. */
  private sealed trait ExecutionStep extends Product with Serializable

  private object ExecutionStep {
    type NewState = Future[Option[RunningMirror]]

    /** Concrete functions that should be executed on the value associated with the given [[MirrorID]]. */
    case class ExecutionCommand(id: MirrorID,
                                whenPresent: RunningMirror => NewState,
                                whenMissing: () => NewState) extends ExecutionStep

    /** Indicates that all mirrors currently running should be stopped. */
    case class StopAll(stop: MirrorID => RunningMirror => Future[None.type]) extends ExecutionStep

    /** That all stopped mirrors should be restarted. */
    case class RestartStopped(start: (MirrorID, ExternalKafkaMirrorSettings) => NewState) extends ExecutionStep
  }

  import ExecutionStep._

  /** Given the state of the Kafka mirrors, executes the given command, this includes side-effects.
    * Produces a new state of mirrors that corresponds to the state of affairs after execution.
    */
  def executeMirrors(makeMirror: MirrorMaker, killSwitch: KillSwitch)
                    (mirrors: Map[MirrorID, RunningMirror],
                     command: MirrorCommand)
                    (implicit executionContext: ExecutionContext,
                     logger: LoggingAdapter): Future[Map[MirrorID, RunningMirror]] = {

    val nextExecutionStep = computeNextExecutionStep(makeMirror)(command)

    nextExecutionStep match {
      case ExecutionCommand(id, whenPresent, whenMissing) =>
        futureUpdateMap(mirrors)(id, whenPresent, whenMissing)

      case StopAll(stop) => stopAll(mirrors, stop, killSwitch)

      case RestartStopped(start) => restartStopped(mirrors, start)
    }
  }

  /** Computing the next execution step given the [[MirrorCommand]].
    * Not actually performing anything at this stage.
    */
  private def computeNextExecutionStep(mirrorMaker: MirrorMaker)
                                      (command: MirrorCommand)
                                      (implicit executionContext: ExecutionContext,
                                       logger: LoggingAdapter): ExecutionStep = {

    val mirrorLifeCycleHandler = new MirrorLifeCycleHandler(mirrorMaker)
    import mirrorLifeCycleHandler._

    command match {
      case Start(id, mirrorSettings) =>
        ExecutionCommand(
          id,
          whenPresent = restart(id, mirrorSettings),
          whenMissing = start(id, mirrorSettings) _
        )

      case Stop(id) =>
        ExecutionCommand(
          id,
          whenPresent = stop(id),
          whenMissing = () => clear
        )

      case Shutdown => StopAll(stop)

      case VerifyState => RestartStopped(start(_, _)())
    }
  }

  /** Wraps around functions needed to start/stop mirrors. */
  private class MirrorLifeCycleHandler(makeMirror: MirrorMaker)
                                      (implicit executionContext: ExecutionContext,
                                       logger: LoggingAdapter) {
    val clear = Future.successful(None)

    def stop(id: MirrorID)
            (mirror: RunningMirror): Future[None.type] = {
      def errorShuttingDown(e: Throwable) = {
        logger.error(e, s"Error while shutting down Kafka mirror: [${id.value}]")
        Done
      }

      logger.info(s"Trying to shutdown Kafka mirror: [${id.value}]")
      mirror.control.shutdown()
        .recover {
          case NonFatal(e) => errorShuttingDown(e)
        }
        .map { _ =>
          logger.info(s"Shutdown Kafka mirror: [${id.value}]")
          None
        }
    }

    def start(id: MirrorID, mirrorSettings: ExternalKafkaMirrorSettings)
             (): Future[Option[RunningMirror]] = {
      def errorStarting(e: Throwable) = {
        logger.error(e, s"Failed to start Kafka mirror: [${id.value}]")
        clear
      }

      mirrorSettings.toKafkaMirrorSettings.map { kafkaSettings =>
        val mirror = KafkaMirror(makeMirror)(id, kafkaSettings)
        mirror.start()
      }.fold(
        errorStarting,
        control => Future.successful(Some(RunningMirror(control, mirrorSettings)))
      )
    }

    def restart(id: MirrorID, mirrorSettings: ExternalKafkaMirrorSettings)
               (mirror: RunningMirror): Future[Option[RunningMirror]] =
      stop(id)(mirror).flatMap { _ =>
        start(id, mirrorSettings)
      }
  }

  /** Restarts all mirrors that are currently stopped. */
  private def restartStopped(mirrors: Map[MirrorID, RunningMirror],
                             start: (MirrorID, ExternalKafkaMirrorSettings) => NewState)
                            (implicit logger: LoggingAdapter,
                             executionContext: ExecutionContext): Future[Map[MirrorID, RunningMirror]] = {
    Future.sequence {
      mirrors.map { case (id, mirror) =>
        if (isShutDownNow(mirror.control)) {
          logger.error(s"Kafka mirror with ID: [${id.value}], terminated unexpectedly, attempting to restart")

          start(id, mirror.mirrorSettings).map(_.map((id, _)))
        }
        else Future.successful(Some((id, mirror)))
      }
    }.map(_.flatten.toMap)
  }

  /** Stops all mirrors and terminates the stream. */
  private def stopAll(mirrors: Map[MirrorID, RunningMirror],
                      stop: MirrorID => RunningMirror => Future[None.type],
                      killSwitch: KillSwitch)
                     (implicit executionContext: ExecutionContext): Future[Map[MirrorID, RunningMirror]] = {
    Future.sequence {
      mirrors.map { case (id, mirror) =>
        stop(id)(mirror)
      }
    }.map { _ =>
      killSwitch.shutdown()
      Map.empty
    }
  }

  def isShutDownNow(control: Control) = control.isShutdown
    .value // we care only whether the system was shutdown now, hence no waiting on the future value
    .map(_.isSuccess)
    .exists(identity)

  /** Tries to fetch a value from the map and apply a function to it. If the value is missing
    * then applying the fallback function.
    * Given the resulting optional value, removing the key from the map if it's [[None]], if not
    * replacing the key's value with the new value.
    */
  def futureUpdateMap[A, B](valuesMap: Map[A, B])
                           (key: A,
                            whenPresent: B => Future[Option[B]],
                            whenMissing: () => Future[Option[B]])
                           (implicit executionContext: ExecutionContext): Future[Map[A, B]] =
    valuesMap.get(key)
      .map(whenPresent)
      .getOrElse(whenMissing())
      .map { maybeNewValue =>
        maybeNewValue.map { newValue =>
          valuesMap.updated(key, newValue)
        }.getOrElse(valuesMap - key)
      }
}
