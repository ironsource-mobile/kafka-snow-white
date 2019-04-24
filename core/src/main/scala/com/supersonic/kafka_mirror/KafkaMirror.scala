package com.supersonic.kafka_mirror

import akka.Done
import akka.event.LoggingAdapter
import akka.kafka.ConsumerMessage.{CommittableMessage, CommittableOffset, CommittableOffsetBatch}
import akka.kafka.ProducerMessage.Message
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.kafka.{ProducerMessage, Subscriptions}
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{ActorAttributes, Materializer, Supervision}
import org.apache.kafka.clients.consumer.CommitFailedException
import org.apache.kafka.clients.producer.{ProducerRecord, Producer => KafkaProducer}
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

  /** Builds a new mirror with the given ID.
    *
    * @param producer The producer that is being used by the mirror. Needed so that
    *                 so that it can be closed upon completion.
    */
  def apply(mirrorID: MirrorID, producer: KafkaProducer[_, _])
           (source: LoggingAdapter => Source[Done, Control]): Mirror
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
                  settings: KafkaMirrorSettings[K, V]): makeMirror.Mirror = {
    // This producer will not be managed by the library, so we have to close it upon stream
    // completion, that's why we pass it to [[makeMirror]].
    val producer = settings.kafka.producer.createKafkaProducer()

    makeMirror(id, producer) { implicit logger =>
      val mirrorName = s"kafka-mirror-${id.value}"

      Consumer.committableSource(
        settings.kafka.consumer,
        Subscriptions.topics(settings.mirror.whitelist))
        .mapConcat(makeMessage[K, V](
          settings.mirror,
          settings.hashKey,
          producer.partitionsFor(_).size,
          settings.generatePartition) _ andThen (_.toList))
        .via(Producer.flexiFlow(settings.kafka.producer, producer))
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
  }

  /** Converts a message from a specific topic in a consumer to the corresponding topic and message in the
    * producer.
    * This includes support for bucketing, as a result it is possible that there will be no messages.
    */
  private[kafka_mirror] def makeMessage[K, V](mirror: MirrorSettings,
                                              hashKey: K => Int,
                                              getNumOfPartitions: String => Int,
                                              generatePartition: Int => Int)
                                             (message: CommittableMessage[K, V]): Option[Message[K, V, CommittableOffset]] = {
    // can be null, leaving it here, since we need to pass it along to Kafka later on
    val rawKey = message.record.key
    val maybeKey = Option(rawKey)

    def shouldSend(bucketSettings: BucketSettings): Boolean =
    // 'null' keys are always mirrored, since bucketing is used for (deterministic) mirroring by key
    // there is no point to mirror 'null' as it will always land in the same bucket.
    // So we consider 'null' to be an indication that the user doesn't need bucketing
      maybeKey
        .forall(key => hashKey(key) % bucketSettings.totalBuckets < bucketSettings.mirrorBuckets)

    def getPartitionNumber(topic: String) = {
      val totalPartitions = getNumOfPartitions(topic)

      def partitionFromKey(key: K) = hashKey(key) % totalPartitions

      def randomPartition = generatePartition(totalPartitions)

      maybeKey
        .filter(_ => mirror.partitionFromKeys)
        .map(partitionFromKey)
        .getOrElse(randomPartition)
    }

    val send = mirror.bucketing.forall(shouldSend)
    if (send) {
      val timestamp: java.lang.Long =
        if (message.record.timestamp == RecordBatch.NO_TIMESTAMP) null else message.record.timestamp

      val topic = mirror.topicsToRename.getOrElse(message.record.topic, message.record.topic)

      val partition = getPartitionNumber(topic)

      Some {
        ProducerMessage.Message(new ProducerRecord[K, V](
          topic,
          partition,
          timestamp,
          rawKey,
          message.record.value),
          message.committableOffset)
      }
    }
    else Option.empty
  }
}

/** A Kafka mirror that is backed by a [[Source]].
  *
  * @param producer The producer that's being used by the mirror. Needed here for cleanup after the
  *                 mirror completes.
  */
case class SourcedKafkaMirror(mirrorID: MirrorID,
                              source: Source[Done, Control],
                              producer: KafkaProducer[_, _],
                              materializer: Materializer,
                              logger: LoggingAdapter) extends KafkaMirror {
  def start(): Control = {
    logger.info(s"Starting Kafka mirror with ID: [${mirrorID.value}]")

    val closeProducer = Sink.onComplete { _ =>
      // note that some of the tests do not use the [[start]] method, which means that this 'close'
      // call will not be performed there.
      producer.close()
    }

    source.toMat(closeProducer)(Keep.left).run()(materializer)
  }
}

/** Creates Kafka mirrors that are backed by a [[Source]] using the provided materializer. */
class SourceBackedMirrorMaker(materializer: Materializer,
                              logger: LoggingAdapter) extends MirrorMaker {
  type Mirror = SourcedKafkaMirror

  def apply(mirrorID: MirrorID, producer: KafkaProducer[_, _])
           (source: LoggingAdapter => Source[Done, Control]) =
    SourcedKafkaMirror(mirrorID, source(logger), producer, materializer, logger)
}
