package com.supersonic.kafka_mirror

import akka.NotUsed
import akka.stream.scaladsl.Flow
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, TimerGraphStageLogic, _}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import scala.concurrent.duration.FiniteDuration

/** Filters events in such a way that incoming events have to wait at least [[staggerTime]] until
  * they are fired.
  *
  * Every time an event occurs a timer is set for [[staggerTime]], if in the meantime there are no
  * new events, then the event is fired. If a new event arrives, then the old event is dropped
  * and the timer is reset. The cycle repeats until no new events arrive and then the last retained
  * event is fired.
  *
  * @param staggerTime The time to wait before firing an event.
  */
class StaggeredEventsFilterGate[A](staggerTime: FiniteDuration) extends GraphStage[FlowShape[A, A]] {

  private val in = Inlet[A]("StaggeredEventsFilterGate.in")
  private val out = Outlet[A]("StaggeredEventsFilterGate.out")

  val shape = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) {
      var last = Option.empty[A]

      def pushLater() = scheduleOnce((), staggerTime)

      setHandler(in, new InHandler {
        def onPush(): Unit = {
          val a = grab(in)
          last = Some(a)

          pushLater()
          pull(in)
        }
      })

      setHandler(out, new OutHandler {
        def onPull() = {
          pushLater()
          if (!hasBeenPulled(in)) pull(in)
        }
      })

      override protected def onTimer(timerKey: Any): Unit = {
        last.foreach { a =>
          if (isAvailable(out)) {
            push(out, a)
            last = None
          }
        }
      }
    }
}

object StaggeredEventsFilterGate {
  def apply[A](staggerTime: FiniteDuration): Flow[A, A, NotUsed] =
    Flow.fromGraph(new StaggeredEventsFilterGate(staggerTime))
}

