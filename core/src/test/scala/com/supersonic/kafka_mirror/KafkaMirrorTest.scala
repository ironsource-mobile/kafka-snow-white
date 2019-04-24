package com.supersonic.kafka_mirror

import akka.kafka.ConsumerMessage._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.record.TimestampType
import org.scalatest.{Inspectors, Matchers, OptionValues, WordSpecLike}

class KafkaMirrorTest extends WordSpecLike
                              with Matchers
                              with Inspectors
                              with OptionValues {

  val bucketing = BucketSettings(
    mirrorBuckets = 3,
    totalBuckets = 5
  )

  val hashKey = (_: String).toInt

  val partitionNum = 5

  val getNumberOfPartitions = (_: String) => partitionNum

  val generatePartition = (n: Int) => n

  def mirrorSettings(topicsToRename: Map[String, String] = Map.empty,
                     bucketSettings: Option[BucketSettings] = None)
                    (whitelist: String*) =
    MirrorSettings(
      whitelist = whitelist.toSet,
      commitBatchSize = 20,
      commitParallelism = 3,
      bucketing = bucketSettings,
      partitionFromKeys = false,
      topicsToRename = topicsToRename)

  "The message producing function" should {
    val topic = "some-topic"
    val renamedTopic = "renamed-topic"

    val offset = new CommittableOffset {
      val partitionOffset = PartitionOffset(GroupTopicPartition("some-group", topic, 1), 13L)

      def commitJavadsl() = ???

      def commitScaladsl() = ???

      def batchSize: Long = ???
    }

    def makeConsumerMessage(key: String) = {
      val record = new ConsumerRecord(topic, 1, 13L, 15L, TimestampType.CREATE_TIME, 17L, 99, 31, key, "the-value")

      CommittableMessage(record, offset)
    }

    def makeRenamedConsumerMessage(key: String) = {
      val record = new ConsumerRecord(renamedTopic, 1, 13L, 15L, TimestampType.CREATE_TIME, 17L, 99, 31, key, "the-value")

      CommittableMessage(record, offset)
    }

    def makeMessage(mirror: MirrorSettings, message: CommittableMessage[String, String]) =
      KafkaMirror.makeMessage(mirror, hashKey, getNumberOfPartitions, generatePartition)(message)

    "create a message for the right topic in the producer" in {
      val settings = mirrorSettings()(topic)
      val consumerMessage = makeConsumerMessage("the-key")

      val maybeMessage = makeMessage(settings, consumerMessage)

      val message = maybeMessage.value

      message.passThrough shouldBe consumerMessage.committableOffset
      message.record.topic shouldBe consumerMessage.record.topic
      message.record.partition shouldBe partitionNum
      message.record.timestamp shouldBe consumerMessage.record.timestamp
      message.record.key shouldBe consumerMessage.record.key
      message.record.value shouldBe consumerMessage.record.value
    }

    "create a message for the right topic in the producer with topic rename" in {
      val settings = mirrorSettings(topicsToRename = Map(topic -> renamedTopic))(topic)
      val consumerMessage = makeConsumerMessage("the-key")
      val renamedConsumerMessage = makeRenamedConsumerMessage("the-key")

      val maybeMessage = makeMessage(settings, consumerMessage)

      val message = maybeMessage.value

      message.passThrough shouldBe renamedConsumerMessage.committableOffset
      message.record.topic shouldBe renamedConsumerMessage.record.topic
      message.record.partition shouldBe partitionNum
      message.record.timestamp shouldBe renamedConsumerMessage.record.timestamp
      message.record.key shouldBe renamedConsumerMessage.record.key
      message.record.value shouldBe renamedConsumerMessage.record.value
    }

    "use deterministic bucketing if bucketing settings are present" in {
      val settingsWithBucketing = mirrorSettings(bucketSettings = Some(bucketing))(List("some-topic"): _*) // avoiding varargs due to a bug in the compiler
      val settingsNoBucketing = mirrorSettings()("some-topic")

      val makeMessageWithBucketing =
        makeMessage(settingsWithBucketing, _: CommittableMessage[String, String])
      val makeMessageWithNoBucketing =
        makeMessage(settingsNoBucketing, _: CommittableMessage[String, String])

      def verifyNoMessage(message: CommittableMessage[String, String]) =
        makeMessageWithBucketing(message) shouldBe empty

      def verifyMessage(message: CommittableMessage[String, String]) =
        makeMessageWithBucketing(message) shouldBe makeMessageWithNoBucketing(message)

      def messages(nums: Int*) = nums.map(_.toString).map(makeConsumerMessage)

      forEvery(messages(1, 2, 5, 6, 7, 10, 11, 12, 15))(verifyMessage)

      forEvery(messages(3, 4, 8, 9, 13, 14))(verifyNoMessage)
    }

    "not use bucketing if the message key is null" in {
      def hashKey(str: String) = bucketing.totalBuckets - 1 // we want to always be not-mirrored

      val settingsWithBucketing = mirrorSettings(bucketSettings = Some(bucketing))(List("some-topic"): _*) // avoiding varargs due to a bug in the compiler
      val settingsNoBucketing = mirrorSettings()("some-topic")

      val makeMessageWithBucketing =
        KafkaMirror.makeMessage[String, String](settingsWithBucketing, hashKey, getNumberOfPartitions, generatePartition) _
      val makeMessageWithNoBucketing =
        KafkaMirror.makeMessage[String, String](settingsNoBucketing, hashKey, getNumberOfPartitions, generatePartition) _

      val message = makeConsumerMessage(null)
      makeMessageWithBucketing(message) shouldBe makeMessageWithNoBucketing(message)
    }

    "handle a missing timestamp in the incoming message" in {
      val settings = mirrorSettings()("some-topic")

      val timestamp = -1L
      val record = new ConsumerRecord(topic, 1, 13L, timestamp, TimestampType.NO_TIMESTAMP_TYPE, 17L, 99, 31, "some-key", "the-value")

      val consumerMessage = CommittableMessage(record, offset)

      val maybeMessage = KafkaMirror.makeMessage(settings, hashKey, getNumberOfPartitions, generatePartition)(consumerMessage)

      val message = maybeMessage.value

      message.record.timestamp shouldBe null
    }

    "generate the partition number from the key when the flag is on and the key is present" in {
      val settings = mirrorSettings()(topic)
        .copy(partitionFromKeys = true)

      val consumerMessage = makeConsumerMessage("3")

      val maybeMessage = makeMessage(settings, consumerMessage)

      val partition = maybeMessage.value.record.partition

      partition should not be partitionNum
      partition shouldBe 3
    }

    "generate the partition number with the generating function" when {
      "the flag is off" in {
        val settings = mirrorSettings()(topic)
          .copy(partitionFromKeys = false)

        val consumerMessage = makeConsumerMessage("3")

        val maybeMessage = makeMessage(settings, consumerMessage)

        val partition = maybeMessage.value.record.partition

        partition shouldBe partitionNum
      }

      "the flag is on but the key is 'null'" in {
        val settings = mirrorSettings()(topic)
          .copy(partitionFromKeys = true)

        val consumerMessage = makeConsumerMessage(key = null)

        val maybeMessage = makeMessage(settings, consumerMessage)

        val partition = maybeMessage.value.record.partition

        partition shouldBe partitionNum
      }
    }
  }
}
