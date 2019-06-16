package com.supersonic.kafka_mirror

import akka.actor.ActorSystem
import akka.event.Logging
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import com.supersonic.kafka_mirror.MirrorCommand._
import com.supersonic.kafka_mirror.TestUtil._
import com.typesafe.config.ConfigFactory
import org.scalatest.{Matchers, WordSpecLike}

class SettingsProcessingFlowTest extends TestKit(ActorSystem("SettingsProcessingFlowTest"))
                                         with WordSpecLike
                                         with Matchers {

  implicit val ec = system.dispatcher
  implicit val materializer = ActorMaterializer()
  implicit val logger = Logging(system.eventStream, "SettingsProcessingFlowTest")

  val fallback = ConfigFactory.parseString("""
    akka.kafka.consumer = {a = 1}
    akka.kafka.producer = {b = 2}
  """)
  val root = "foo"

  def probes() = {
    val flow = SettingsProcessingFlow(fallback)

    TestUtil.probes(flow)
  }

  def settings(i: Int) = Some {
    s"""
    consumer = {a = $i}
    producer = {b = $i}
    mirror = {
      whitelist = ["topic1", "topic2"]
      commitBatchSize = 1
      commitParallelism = 4
    }
    """
  }

  def parsedSettings(idStr: String, i: Int) =
    SettingsProcessingFlow.fromRawMap(fallback) {
      Map(id(idStr) -> settings(i))
    }(id(idStr)).getOrElse(sys.error("should not happen"))

  "The settings processing flow" should {
    "process settings changes" in {
      val (pub, sub) = probes()

      pub.sendNext(Map(
        id("a") -> settings(1),
        id("b") -> settings(2)
      ))

      pub.sendNext(Map(
        id("a") -> settings(1),
        id("b") -> settings(3)
      ))

      pub.sendNext(Map(
        id("b") -> settings(3)
      ))

      pub.sendNext(Map(
        id("b") -> None
      ))

      sub.request(2)
      sub.expectNextUnordered(
        Start(id("a"), parsedSettings("a", 1)),
        Start(id("b"), parsedSettings("b", 2))
      )

      sub.request(1)
      sub.expectNext(
        Start(id("b"), parsedSettings("b", 3))
      )

      sub.request(1)
      sub.expectNext(
        Stop(id("a"))
      )

      sub.request(1)
      sub.expectNext(
        Stop(id("b"))
      )
    }

    "stop mirrors whose settings failed to parse" in {
      val (pub, sub) = probes()

      pub.sendNext(Map(
        id("a") -> settings(1)
      ))

      sub.request(1)
      sub.expectNext(
        Start(id("a"), parsedSettings("a", 1))
      )

      pub.sendNext(Map(
        id("b") -> Some("bla =")
      ))

      sub.request(1)
      sub.expectNext(
        Stop(id("a"))
      )
    }
  }
}
