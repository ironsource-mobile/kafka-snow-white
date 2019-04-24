package com.supersonic.kafka_mirror

import java.util.concurrent.ThreadLocalRandom
import akka.kafka.{ConsumerSettings, ProducerSettings}
import com.typesafe.config.Config
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.apache.kafka.common.utils.Utils
import scala.util.Try

/** Note that equality doesn't work on this class since the ***Settings classes don't override
  * the equals method.
  */
case class KafkaSettings[K, V](consumer: ConsumerSettings[K, V],
                               producer: ProducerSettings[K, V])

/** Settings for a Kafka mirror.
  *
  * @param whitelist         The topics that should be mirrored, i.e., the same topics on in the source
  *                          will be mirrored to the target.
  * @param commitBatchSize   How many messages to batch before committing their offsets to Kafka.
  * @param commitParallelism The parallelism level used to commit the offsets.
  * @param bucketing         Settings to enable bucketing of mirrored values.
  * @param enabled           Whether the mirror should be enabled or not.
  * @param partitionFromKeys Whether the partition number when mirroring should be computed from the
  *                          key or not.
  * @param topicsToRename    Map of src to dest topic to rename when mirroring the message to the producer.
  *
  */
case class MirrorSettings(whitelist: Set[String],
                          commitBatchSize: Int = 1000,
                          commitParallelism: Int = 4,
                          bucketing: Option[BucketSettings] = None,
                          enabled: Boolean = true,
                          partitionFromKeys: Boolean = false,
                          topicsToRename: Map[String, String] = Map.empty)

/**
  * Defines settings for mirroring buckets.
  * The (mirrorBuckets / totalBuckets) ratio is the percentage of traffic to be mirrored.
  *
  * @param mirrorBuckets The number of buckets that should be mirrored.
  * @param totalBuckets  The total number of buckets, used to calculate the percentage of traffic
  *                      to mirror.
  */
case class BucketSettings(mirrorBuckets: Int, totalBuckets: Int) {
  assert(mirrorBuckets <= totalBuckets, "The number of mirroring buckets cannot exceed the total number of buckets")
}

/** Note that equality doesn't work on this class since the [[KafkaSettings]] doesn't have
  * a good equality method.
  *
  * @param generatePartition Takes the total number of partitions and generates a random partition
  *                          number (will be random in actual usage).
  */
case class KafkaMirrorSettings[K, V](kafka: KafkaSettings[K, V],
                                     mirror: MirrorSettings,
                                     hashKey: K => Int,
                                     generatePartition: Int => Int)

/** Copying the [[KafkaMirrorSettings]] hierarchy, but instead of using the dedicated consumer/producer
  * settings objects using [[Config]]s, this enables logical equality on these classes (since
  * the dedicated classes are not defining a custom equality method).
  */
case class ExternalKafkaSettings(consumer: Config, producer: Config)
case class ExternalKafkaMirrorSettings(kafka: ExternalKafkaSettings, mirror: MirrorSettings) {

  def toKafkaMirrorSettings: Try[KafkaMirrorSettings[Array[Byte], Array[Byte]]] = Try {
    val consumerSettings = ConsumerSettings(kafka.consumer, new ByteArrayDeserializer, new ByteArrayDeserializer)
    val producerSettings = ProducerSettings(kafka.producer, new ByteArraySerializer, new ByteArraySerializer)

    def hashKey(k: Array[Byte]) =
      Utils.abs(Utils.murmur2(if (k == null) Array.empty else k))

    def generatePartition(totalPartitions: Int) =
      ThreadLocalRandom.current().nextInt(totalPartitions)

    KafkaMirrorSettings(
      KafkaSettings(consumerSettings, producerSettings),
      mirror,
      hashKey,
      generatePartition)
  }
}
