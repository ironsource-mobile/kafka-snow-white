package com.supersonic.kafka_mirror

import akka.actor.ActorSystem
import org.scalatest.{Matchers, WordSpec}

class ExternalKafkaMirrorSettingsTest extends WordSpec with Matchers {
  implicit val system = ActorSystem("ExternalKafkaMirrorSettingsTest")

  "The Kafka mirrors settings hashing function" should {
    val hashFunction = TestUtil.runnableSettings("test")
      .toKafkaMirrorSettings.get.hashKey

    def bytes = Array(1, 2, 3, 4).map(_.toByte)

    val bytes1 = bytes
    val bytes2 = bytes

    "not use object identity for hashing" in {
      bytes1 shouldNot be theSameInstanceAs bytes2
      hashFunction(bytes1) shouldEqual hashFunction(bytes2)
    }

    "compute the hash using the byte contents" in {
      hashFunction(bytes) shouldEqual 1698034171
    }

    "produce positive numbers" in {
      hashFunction(Array(1, 2).map(_.toByte)) should be >= 0
    }

    "not fail if the input is 'null'" in {
      noException should be thrownBy hashFunction(null)
    }
  }
}
