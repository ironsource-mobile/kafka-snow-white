package com.supersonic.kafka_mirror

import akka.actor.ActorSystem
import akka.pattern
import akka.stream.ActorMaterializer
import akka.stream.testkit.TestPublisher
import akka.testkit.TestKit
import com.supersonic.kafka_mirror.TestUtil._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, WordSpecLike}
import scala.concurrent.Future
import scala.concurrent.duration._

class StaggeredEventsFilterGateTest extends TestKit(ActorSystem("StaggeredEventsFilterGateTest"))
                                            with WordSpecLike
                                            with Matchers
                                            with ScalaFutures {
  override implicit val patienceConfig = PatienceConfig(30.second)
  implicit val ec = system.dispatcher
  implicit val materializer = ActorMaterializer()

  def gate(staggerTime: FiniteDuration) = StaggeredEventsFilterGate[Int](staggerTime)

  def sendAfter[A](probe: TestPublisher.Probe[A])(duration: FiniteDuration)(value: A) =
    pattern.after(duration, using = system.scheduler) {
      probe.sendNext(value)
      Future.successful(duration)
    }

  def sleep(duration: FiniteDuration) =
    pattern.after(duration, using = system.scheduler) {
      Future.successful(duration)
    }

  "The staggered events filter gate" should {
    "pass the first element if there aren't any other coming" in {
      val (pub, sub) = probes(gate(500.millis))

      sub.request(1)
      pub.sendNext(1)

      sub.expectNext(1)
    }

    "hold on to the first element for the stagger duration" in {
      val (pub, sub) = probes(gate(2.second))

      sub.request(1)
      pub.sendNext(1)

      an[AssertionError] should be thrownBy {
        sub.expectNext(1.second)
      }
    }

    "pass on the last element when multiple events appear together" in {
      val (pub, sub) = probes(gate(500.millis))

      sub.request(1)

      pub.sendNext(1)
      pub.sendNext(2)
      pub.sendNext(3)

      sub.expectNext(3)
    }

    "pass on the last element when multiple events appear within less of the stagger time of each other" in {
      val (pub, sub) = probes(gate(1.second))
      val send = sendAfter(pub)(100.millis) _

      sub.request(1)

      for {
        _ <- send(1)
        _ <- send(2)
        _ <- send(3)
      } yield ()

      sub.expectNext(3)
    }

    "wait between a groups of incoming events that that a separated by the stagger time" in {
      val (pub, sub) = probes(gate(1.second))
      val send = sendAfter(pub)(100.millis) _

      sub.request(2)

      for {
        _ <- send(1)
        _ <- send(2)
        _ <- send(3)
        _ <- sleep(2.second)
        _ <- send(4)
        _ <- send(5)
        _ <- send(6)
      } yield ()

      sub.expectNext(3, 6)
    }

    "reset the timer even if the total wait goes over the stagger time" in {
      val staggerTime = 500.millis
      val (pub, sub) = probes(gate(staggerTime))
      val send = sendAfter(pub)(200.millis) _

      sub.request(1)

      val totalTime = for {
        t1 <- send(1)
        t2 <- send(2)
        t3 <- send(3)
        t4 <- send(4)
        t5 <- send(5)
        t6 <- send(6)
      } yield List(t1, t2, t3, t4, t5, t6).reduce(_ + _)

      totalTime.futureValue should be > staggerTime

      sub.expectNext(6)
    }

    "not query the source when there isn't any demand" in {
      val (pub, sub) = probes(gate(200.millis))
      val send = sendAfter(pub)(250.millis) _

      sub.request(2)

      pub.sendNext(1)
      val sent = send(2).map { _ =>
        val _ = pub.sendError(new Exception("shouldn't be read"))
      }

      whenReady(sent) { _ =>
        sub.expectNext(1)
      }
    }

    "not push events when they weren't requested" in {
      val (pub, sub) = probes(gate(100.millis))

      sub.request(1)
      pub.sendNext(1)

      sub.expectNext(1)

      pub.sendNext(2)

      // note that we didn't request a message here
      noException should be thrownBy {
        sub.expectNoMessage(200.millis)
      }
    }

    "produce events that were received before they were requested" in {
      val (pub, sub) = probes(gate(100.millis))

      sub.request(1)
      pub.sendNext(1)

      sub.expectNext(1)

      pub.sendNext(2)

      // note that we didn't request a message here
      sub.expectNoMessage(200.millis)

      sub.request(1)
      sub.expectNext(2)
    }
  }
}
