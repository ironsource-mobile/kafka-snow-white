package com.supersonic.kafka_mirror

import akka.actor.ActorSystem
import akka.event.Logging
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import com.supersonic.kafka_mirror.MirrorCommand._
import com.supersonic.kafka_mirror.TestUtil._
import org.scalatest.{AsyncWordSpecLike, Matchers}

class MirrorManagerFlowTest extends TestKit(ActorSystem("MirrorManagerFlowTest"))
                                    with AsyncWordSpecLike
                                    with Matchers {

  implicit val ec = system.dispatcher
  implicit val materializer = ActorMaterializer()
  implicit val logger = Logging(system.eventStream, "SettingsProcessingFlowTest")

  val mirrorMaker = new MockMirrorMaker(new MockKafkaMirror)
  val flow = new MirrorManager(mirrorMaker).flow

  def probes() =
    TestUtil.probes(flow)

  "The mirror manager flow" should {
    "start and stop mirrors" in {
      val (pub, sub) = probes()

      pub.sendNext(Start(id("a"), runnableSettings("a")))

      sub.request(2)

      sub.expectNext() shouldBe empty

      val s1 = sub.expectNext()

      s1 should contain key id("a")

      val control = s1(id("a")).control

      pub.sendNext(Stop(id("a")))

      sub.request(1)
      val s2 = sub.expectNext()

      s2 shouldBe empty

      control.isShutdown.map { _ =>
        succeed
      }
    }

    "restart failed mirrors" in {
      val (pub, sub) = probes()

      pub.sendNext(Start(id("a"), runnableSettings("a")))

      sub.request(2)

      sub.expectNext()

      val s1 = sub.expectNext()
      val control = s1(id("a")).control

      control.shutdown().map { _ =>
        MirrorManager.isShutDownNow(control) shouldBe true
        pub.sendNext(VerifyState)

        sub.request(1)
        val s2 = sub.expectNext()

        s2.toList should have size 1

        MirrorManager.isShutDownNow(s2(id("a")).control) shouldBe false
      }
    }

    "shutdown all mirrors and complete the stream" in {
      val (pub, sub) = probes()

      pub.sendNext(Start(id("a"), runnableSettings("a")))
      pub.sendNext(Start(id("b"), runnableSettings("b")))
      pub.sendNext(Start(id("c"), runnableSettings("c")))

      sub.request(4)

      sub.expectNext() shouldBe empty
      sub.expectNext()
      sub.expectNext()

      val state = sub.expectNext()

      state should contain key id("a")
      state should contain key id("b")
      state should contain key id("c")

      val c1 = state(id("a")).control
      val c2 = state(id("b")).control
      val c3 = state(id("c")).control

      pub.sendNext(Shutdown)

      sub.request(1)
      sub.expectComplete()

      for {
        _ <- c1.isShutdown
        _ <- c2.isShutdown
        _ <- c3.isShutdown
      } yield {
        succeed
      }
    }
  }
}
