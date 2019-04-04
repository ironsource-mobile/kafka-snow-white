package com.supersonic.kafka_mirror

import akka.Done
import akka.event.LoggingAdapter
import akka.kafka.ConsumerMessage.{CommittableMessage, CommittableOffset, CommittableOffsetBatch}
import akka.kafka.ProducerMessage.Message
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.kafka.{ProducerMessage, Subscriptions}
import akka.stream.{ActorAttributes, Materializer, Supervision}
import akka.stream.scaladsl.{Keep, Sink, Source}
import org.apache.kafka.clients.consumer.CommitFailedException
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.record.RecordBatch

/** Represents a mirror between two Kafka topics.
  * The mirror can be started, an action that produces a [[Control]] through which the mirroring
  * can be shut down.
  */
trait KafkaMirror {
  /** Starts the mirror described by this instance and returns a [[Control]] to the process. */
  def start(): Control
}

/** Encapsulates the logic for the creation of Kafka mirrors. */
trait MirrorMaker {
  type Mirror <: KafkaMirror

  def apply(mirrorID: MirrorID)(source: LoggingAdapter => Source[Done, Control]): Mirror
}

object KafkaMirror {
  /** A workaround for https://github.com/akka/alpakka-kafka/issues/755
    * Can be removed once Alpakka Kafka 1.0.2 is released.
    */
  private val recoverCommitErrors: Supervision.Decider = {
    case _: CommitFailedException => Supervision.Resume
    case _ => Supervision.Stop
  }

  /** Creates a mirror that streams data from the source topic to the target topic as
    * specified in the settings object.
    */
  def apply[K, V](makeMirror: MirrorMaker)
                 (id: MirrorID,
                  settings: KafkaMirrorSettings[K, V]): makeMirror.Mirror = makeMirror(id) { implicit logger =>
    val mirrorName = s"kafka-mirror-${id.value}"
    Consumer.committableSource(
      settings.kafka.consumer,
      Subscriptions.topics(settings.mirror.whitelist))
      .mapConcat(makeMessage[K, V](settings.mirror, settings.hashKey) _ andThen (_.toList))
      .via(Producer.flexiFlow(settings.kafka.producer))
      .map(_.passThrough)
      .batch(
        max = settings.mirror.commitBatchSize,
        CommittableOffsetBatch.empty.updated)(_ updated _)
      // TODO or should it be producerSettings.parallelism?
      .mapAsync(settings.mirror.commitParallelism)(_.commitScaladsl())
      .withAttributes(ActorAttributes.supervisionStrategy(recoverCommitErrors))
      .named(mirrorName) // to help debugging
      .log(mirrorName)
  }

  /** Converts a message from a specific topic in a consumer to the corresponding topic and message in the
    * producer.
    * This includes support for bucketing, as a result it is possible that there will be no messages.
    */
  private[kafka_mirror] def makeMessage[K, V](mirror: MirrorSettings, hashKey: K => Int)
                                             (message: CommittableMessage[K, V]): Option[Message[K, V, CommittableOffset]] = {
    def shouldSend(bucketSettings: BucketSettings): Boolean = {
      val key = message.record.key

      // 'null' keys are always mirrored, since bucketing is used for (deterministic) mirroring by key
      // there is no point to mirror 'null' as it will always land in the same bucket.
      // So we consider 'null' to be an indication that the user doesn't need mirroring
      key == null ||
        (hashKey(key) % bucketSettings.totalBuckets < bucketSettings.mirrorBuckets)
    }

    val send = mirror.bucketing.forall(shouldSend)
    if (send) {
      val timestamp: java.lang.Long =
        if (message.record.timestamp == RecordBatch.NO_TIMESTAMP) null else message.record.timestamp

      val topic = mirror.topicsToRename.getOrElse(message.record.topic, message.record.topic)

      Some(ProducerMessage.Message(new ProducerRecord[K, V](
        topic,
        null, // we do not pass on the partition, since we do not know how many partitions there are in the target topic
        timestamp,
        message.record.key,
        message.record.value
      ), message.committableOffset))
    }
    else Option.empty
  }
}

/** A Kafka mirror that is backed by a [[Source]]. */
case class SourcedKafkaMirror(mirrorID: MirrorID,
                              source: Source[Done, Control],
                              materializer: Materializer,
                              logger: LoggingAdapter) extends KafkaMirror {
  def start(): Control = {
    logger.info(s"Starting Kafka mirror with ID: [${mirrorID.value}]")
    source.toMat(Sink.ignore)(Keep.left).run()(materializer)
  }
}

/** Creates Kafka mirrors that are backed by a [[Source]] using the provided materializer. */
class SourceBackedMirrorMaker(materializer: Materializer,
                              logger: LoggingAdapter) extends MirrorMaker {
  type Mirror = SourcedKafkaMirror

  def apply(mirrorID: MirrorID)
           (source: LoggingAdapter => Source[Done, Control]) =
    SourcedKafkaMirror(mirrorID, source(logger), materializer, logger)
}
