package com.supersonic.kafka_mirror

import akka.actor.ActorSystem
import akka.event.Logging
import akka.stream.KillSwitches
import com.supersonic.kafka_mirror.MirrorCommand._
import com.supersonic.kafka_mirror.MirrorManager._
import com.supersonic.kafka_mirror.TestUtil.{settings, _}
import org.scalatest.{AsyncWordSpec, Inspectors, Matchers}
import scala.concurrent.Future

class MirrorManagerHelpersTest extends AsyncWordSpec with Matchers with Inspectors {
  implicit val system = ActorSystem("MirrorManagerHelpersTest")
  implicit val logger = Logging(system.eventStream, "MirrorManagerHelpersTest")

  "Executing the mirror commands" when {
    def execute(newMirror: => KafkaMirror = new MockKafkaMirror) =
      executeMirrors(new MockMirrorMaker(newMirror), KillSwitches.shared("kill-switch")) _

    "receiving the start command it" should {
      "restart the mirror if it's present" in {
        val newMirror = new MockKafkaMirror
        val command = Start(id("a"), runnableSettings("a"))

        val control = new MockControl
        val mirror = RunningMirror(control, settings("a"))

        val state = Map(id("a") -> mirror)

        execute(newMirror)(state, command).map { newState =>
          control.isShutdownNow() shouldBe true
          newState should contain key id("a")
          newMirror.isStarted() shouldBe true
          newState(id("a")).control shouldBe newMirror.control.get
        }
      }

      "start the mirror it isn't present" in {
        val newMirror = new MockKafkaMirror
        val command = Start(id("a"), runnableSettings("a"))

        execute(newMirror)(Map.empty, command).map { newState =>
          newState should contain key id("a")
          newMirror.isStarted() shouldBe true
          newState(id("a")).control shouldBe newMirror.control.get
        }
      }

      "fail to start when required Kafka settings are missing" in {
        val s = settings("a") // the settings should not be runnable
        an[Exception] should be thrownBy s.toKafkaMirrorSettings.get

        execute()(Map.empty, Start(id("a"), s)).map { newState =>
          newState shouldBe empty
        }
      }

      "fail to start when the mirror fails to start" in {
        val command = Start(id("a"), runnableSettings("a"))

        execute(FailingKafkaMirror)(Map.empty, command).map { newState =>
          newState shouldBe empty
        }
      }
    }

    "receiving the stop command it" should {
      "stop the mirror if it is present and remove it" in {
        val control = new MockControl
        val mirror = RunningMirror(control, settings("a"))

        val command = Stop(id("a"))

        val state = Map(id("a") -> mirror)

        execute()(state, command).map { newState =>
          control.isShutdownNow() shouldBe true
          newState shouldNot contain key id("a")
        }
      }

      "do nothing if it isn't" in {
        val command = Stop(id("a"))

        execute()(Map.empty, command).map { newState =>
          newState shouldBe empty
        }
      }

      "remove mirrors that failed to stop" in {
        val mirror = RunningMirror(FailingControl, settings("a"))

        val command = Stop(id("a"))

        val state = Map(id("a") -> mirror)

        execute()(state, command).map { newState =>
          newState shouldNot contain key id("a")
        }
      }
    }

    "receiving the shutdown command it" should {
      "stop all current mirrors and remove them" in {
        val c1, c2, c3, c4 = new MockControl

        val m1 = RunningMirror(c1, settings("a"))
        val m2 = RunningMirror(c2, settings("b"))
        val m3 = RunningMirror(c3, settings("c"))
        val m4 = RunningMirror(c4, settings("d"))

        val command = Shutdown

        val state = Map(
          id("a") -> m1,
          id("b") -> m2,
          id("c") -> m3,
          id("d") -> m4
        )

        execute()(state, command).map { newState =>
          forAll(List(c1, c2, c3, c4)) {
            _.isShutdownNow() shouldBe true
          }
          newState shouldBe empty
        }
      }
    }

    "receiving a verify command it" should {
      "restart all stopped mirrors" in {
        val command = VerifyState

        val c1, c2, c3, c4, c5, c6 = new MockControl

        val state = Map(
          id("a") -> RunningMirror(c1, runnableSettings("a")),
          id("b") -> RunningMirror(c2, runnableSettings("b")),
          id("c") -> RunningMirror(c3, runnableSettings("c")),
          id("d") -> RunningMirror(c4, runnableSettings("d")),
          id("e") -> RunningMirror(c5, runnableSettings("e")),
          id("f") -> RunningMirror(c6, runnableSettings("f"))
        )

        c2.shutdownNow()
        c4.shutdownNow()
        c6.shutdownNow()

        execute()(state, command).map { newState =>
          newState.keySet shouldBe Set(id("a"), id("b"), id("c"), id("d"), id("e"), id("f"))

          val controls = Set(id("b"), id("d"), id("f")).map(newState).map(_.control)

          forAll(controls) { c =>
            MirrorManager.isShutDownNow(c) shouldBe false
          }

          forAll(List(c1, c3, c5)) { c =>
            c.isShutdownNow() shouldBe false
          }
        }
      }
    }
  }

  "Future map updating" when {
    val map = Map(
      1 -> "a",
      2 -> "b",
      3 -> "c")

    val mapOver = futureUpdateMap[Int, String](map) _

    val clear = Future.successful(None)

    "the value is present it" should {
      "apply the 'whenPresent' function and update the key if the result is present" in {
        mapOver(2,
          s => Future.successful(Some(s ++ "-after")),
          () => clear
        ).map { result =>
          result shouldBe Map(
            1 -> "a",
            2 -> "b-after",
            3 -> "c"
          )
        }
      }

      "apply then 'whenPresent' function and remove the key if the result is missing" in {
        mapOver(2,
          _ => clear,
          () => clear
        ).map { result =>
          result shouldBe Map(
            1 -> "a",
            3 -> "c"
          )
        }
      }
    }

    "the value is missing it" should {
      "apply the 'whenMissing' function and set the key if the result is present" in {
        mapOver(4,
          _ => clear,
          () => Future.successful(Some("d"))
        ).map { result =>
          result shouldBe Map(
            1 -> "a",
            2 -> "b",
            3 -> "c",
            4 -> "d"
          )
        }
      }

      "apply the 'whenMissing' function and return the original map" in {
        mapOver(4,
          _ => clear,
          () => clear
        ).map { result =>
          result shouldBe Map(
            1 -> "a",
            2 -> "b",
            3 -> "c"
          )
        }
      }
    }
  }
}
