package com.supersonic.kafka_mirror

import cats.scalatest.{ValidatedMatchers, ValidatedValues}
import com.supersonic.kafka_mirror.MirrorCommand._
import com.supersonic.kafka_mirror.SettingsProcessingFlow._
import com.supersonic.kafka_mirror.TestUtil._
import com.typesafe.config.ConfigFactory
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import org.scalatest.{Matchers, WordSpec}
import com.softwaremill.quicklens._

class SettingsProcessingFlowHelpersTest extends WordSpec with
                                                Matchers with
                                                ValidatedMatchers with
                                                ValidatedValues {

  val fallback = ConfigFactory.parseString("""
      akka.kafka.consumer = { a = 1 }
      akka.kafka.producer = { b = 2 }
  """)

  val (consumerString, producerString) = ("c = 3", "d = 4")
  val consumer = ConfigFactory.parseString(consumerString)
  val producer = ConfigFactory.parseString(producerString)

  val mirrorString = """
    mirror = {
      whitelist = ["topic1", "topic2"]
      commitBatchSize = 1
      commitParallelism = 4
      enabled = true
    }
  """

  val mirror = ConfigFactory.parseString(mirrorString).as[MirrorSettings]("mirror")

  "Computing new mirror commands" should {
    "produce no commands when the state is unchanged" in {
      val state = Map(id("a") -> settings("a"))
      computeCommands(state, state) shouldBe empty
    }

    "if the state of a mirror changes, stop and then start it with the new settings" in {
      val state1 = Map(id("a") -> settings("a"))
      val state2 = Map(id("a") -> settings("b"))

      computeCommands(state1, state2) shouldBe List(Start(id("a"), settings("b")))
    }

    "if a mirror is not present anymore, stop it" in {
      val state = Map(id("a") -> settings("a"))

      computeCommands(state, Map.empty) shouldBe List(Stop(id("a")))
    }

    "if a new mirror appears, start it" in {
      val state = Map(id("a") -> settings("a"))

      computeCommands(Map.empty, state) shouldBe List(Start(id("a"), settings("a")))
    }

    "do nothing when no mirrors are present" in {
      computeCommands(Map.empty, Map.empty) shouldBe empty
    }

    "if a new mirror appears in a disabled state, ignore it" in {
      val sets = settings("a").modify(_.mirror.enabled).setTo(false)
      val state = Map(id("a") -> sets)

      computeCommands(Map.empty, state) shouldBe empty
    }

    "if a mirror's state changes to the disabled state, stop it" in {
      val sets1 = settings("a").modify(_.mirror.enabled).setTo(true)
      val sets2 = settings("a").modify(_.mirror.enabled).setTo(false)
      val state1 = Map(id("a") -> sets1)
      val state2 = Map(id("a") -> sets2)

      computeCommands(state1, state2) shouldBe List(Stop(id("a")))
    }

    "if a mirror's state changes from disabled to enabled, start it" in {
      val sets1 = settings("a").modify(_.mirror.enabled).setTo(false)
      val sets2 = settings("a").modify(_.mirror.enabled).setTo(true)
      val state1 = Map(id("a") -> sets1)
      val state2 = Map(id("a") -> sets2)

      computeCommands(state1, state2) shouldBe List(Start(id("a"), sets2))
    }

    "all together now" in {
      val state1 = Map(
        id("a") -> settings("a"),
        id("b") -> settings("b"),
        id("c") -> settings("c")
      )

      val state2 = Map(
        id("a") -> settings("a"),
        id("b") -> settings("b1"),
        id("d") -> settings("d")
      )

      val commands = computeCommands(state1, state2)

      commands should have size 3

      // we can't rely on the fact that the commands for different IDs appear in any particular order
      commands should contain(Start(id("b"), settings("b1")))
      commands should contain(Stop(id("c")))
      commands should contain(Start(id("d"), settings("d")))
    }
  }

  "Parsing settings from configs" should {
    val settings = fromConfig(fallback)(consumer, producer, mirror).value

    "use the correct fallback for consumers" in {
      val consumerSettings = settings.kafka.consumer

      consumerSettings.getInt("a") shouldBe 1
      consumerSettings.getInt("c") shouldBe 3
    }

    "use the correct fallback for producers" in {
      val producerSettings = settings.kafka.producer

      producerSettings.getInt("b") shouldBe 2
      producerSettings.getInt("d") shouldBe 4
    }

    "pass on the mirror settings" in {
      settings.mirror shouldBe mirror
    }

    "fail when the fallback does not contain the relevant akka/kafka keys" in {
      val fallback1 = ConfigFactory.parseString("akka.kafka.consumer = { a = 1 }")
      val settings1 = fromConfig(fallback1)(consumer, producer, mirror)

      settings1 shouldBe invalid

      val fallback2 = ConfigFactory.parseString("akka.kafka.producer = { a = 1 }")
      val settings2 = fromConfig(fallback2)(consumer, producer, mirror)

      settings2 shouldBe invalid
    }
  }

  "Parsing settings from raw data" when {
    val parse = fromRawMap(fallback) _

    def failParse(data: Map[MirrorID, Option[String]]) = {
      val parsed = parse(data)
      parsed should have size 1
      parsed should contain key MirrorID("bar")
      parsed(MirrorID("bar")) shouldBe invalid
    }

    "receiving a single key in HOCON form" should {
      val dataRoot = Map(
        MirrorID("bar") -> Some(s"""
        consumer = {$consumerString}
        producer = {$producerString}
        $mirrorString
        """)
      )

      def remove(str: String) =
        dataRoot.mapValues(_.map(_.replace(str, "")))

      "parse the settings" in {
        parse(dataRoot) shouldBe Map(
          MirrorID("bar") -> fromConfig(fallback)(consumer, producer, mirror))
      }

      "fail" when {
        "the consumer key is missing" in {
          failParse(remove(s"consumer = {$consumerString}"))
        }

        "the producer key is missing" in {
          failParse(remove(s"producer = {$producerString}"))
        }

        "the mirror key is missing" in {
          failParse(remove(mirrorString))
        }

        "configuration is malformed" in {
          failParse(dataRoot.updated(MirrorID("bar"), Some("bla = ")))
        }

        "one of the sub-keys is missing" in {
          failParse(dataRoot.updated(MirrorID("bar"), None))
        }
      }
    }

    "support parsing multiple mirrors at a time" in {
      val mirrorConfig = Some(s"""
        consumer = {$consumerString}
        producer = {$producerString}
        $mirrorString
      """)

      val data = Map(
        MirrorID("a") -> mirrorConfig,
        MirrorID("b") -> mirrorConfig)

      val settings = fromConfig(fallback)(consumer, producer, mirror)
      parse(data) shouldBe Map(
        MirrorID("a") -> settings,
        MirrorID("b") -> settings
      )
    }
  }
}
