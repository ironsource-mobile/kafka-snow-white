package com.supersonic.kafka_mirror

import java.util.UUID
import akka.NotUsed
import akka.kafka.ProducerMessage.Message
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.kafka.{ConsumerSettings, ProducerSettings, Subscriptions}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKitBase
import kafka.server.KafkaServer
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.zookeeper.server.ServerCnxnFactory
import org.scalatest.{BeforeAndAfterAll, TestSuite}
import scala.collection.immutable
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.reflect.io.Directory

/** A base trait for tests that need to use an embedded Kafka instance together with Akka streams. */
trait AkkaStreamsKafkaIntegrationSpec extends TestKitBase
                                              with TestSuite
                                              with BeforeAndAfterAll {
  implicit val ec = system.dispatcher
  implicit val materializer = ActorMaterializer()(system)

  private var kafkaConnections: List[(ServerCnxnFactory, KafkaServer)] = List.empty

  // TODO make configurable in inheriting classes
  val embeddedKafkaConfig1 = EmbeddedKafkaConfig(kafkaPort = 6001, zooKeeperPort = 6000)
  val embeddedKafkaConfig2 = EmbeddedKafkaConfig(kafkaPort = 6003, zooKeeperPort = 6002)

  val kafkaHelper1 = new KafkaHelper(embeddedKafkaConfig1)
  val kafkaHelper2 = new KafkaHelper(embeddedKafkaConfig2)

  def uuid() = UUID.randomUUID().toString

  def createTopicOnServers(number: Int) = {
    val topic = s"topic$number-" + uuid()

    EmbeddedKafka.createCustomTopic(topic)(embeddedKafkaConfig1)
    EmbeddedKafka.createCustomTopic(topic)(embeddedKafkaConfig2)

    topic
  }

  def createGroup(number: Int) = s"group$number-" + uuid()

  def createProbe(consumerSettings: ConsumerSettings[String, String],
                  topic: String): TestSubscriber.Probe[String] = {
    Consumer.committableSource(consumerSettings, Subscriptions.topics(topic))
      .mapAsync(5) { message =>
        message.committableOffset.commitScaladsl().map { _ =>
          message.record.value
        }
      }
      .runWith(TestSink.probe)
  }

  def produceMessages(topic: String, messages: Range) =
    Await.result(kafkaHelper1.produce(topic, messages), Duration.Inf)

  def mirrorSettings(group: String, bucketSettings: Option[BucketSettings] = None)(whitelist: String*) =
    KafkaMirrorSettings(
      KafkaSettings(kafkaHelper1.createConsumerSettings(group), kafkaHelper2.producerSettings),
      MirrorSettings(
        whitelist = whitelist.toSet,
        commitBatchSize = 20,
        commitParallelism = 3,
        bucketSettings),
      (_: String).toInt)

  def verifyTopic(topic: String, group: String, messages: immutable.Seq[Int]) = {
    val targetTopicProbe = createProbe(kafkaHelper2.createConsumerSettings(group), topic)

    targetTopicProbe
      .request(messages.size)

    messages.foreach { elem =>
      targetTopicProbe.expectNext(10.seconds, elem.toString)
    }

    targetTopicProbe.request(messages.size)
    targetTopicProbe.expectNoMessage(3.seconds)

    targetTopicProbe.cancel()
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    startKafka()
  }

  override def afterAll(): Unit = {
    shutdown(system, 30.seconds)
    shutdownKafka()
    super.afterAll()
  }

  private def startKafka() = {
    kafkaConnections = List(embeddedKafkaConfig1, embeddedKafkaConfig2).map { conf =>
      val zkLogsDir = Directory.makeTemp("zookeeper-logs")
      val kafkaLogsDir = Directory.makeTemp("kafka-logs")

      (EmbeddedKafka.startZooKeeper(conf.zooKeeperPort, zkLogsDir),
        EmbeddedKafka.startKafka(conf, kafkaLogsDir))
    }
  }

  private def shutdownKafka() =
    kafkaConnections.foreach { case (zookeeper, broker) =>
      broker.shutdown()
      zookeeper.shutdown()
    }

  class KafkaHelper(embeddedKafkaConfig: EmbeddedKafkaConfig) {
    val bootstrapServers = s"localhost:${embeddedKafkaConfig.kafkaPort}"

    val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
      .withBootstrapServers(bootstrapServers)

    val partition0 = 0

    /** Produce messages to topic using specified range and return
      * a Future so the caller can synchronize consumption.
      */
    def produce(topic: String, range: Range) = {
      val source = Source(range)
        .map(n => {
          val record = new ProducerRecord(topic, partition0, n.toString, n.toString)

          Message(record, NotUsed)
        })
        .viaMat(Producer.flow(producerSettings))(Keep.right)

      source.runWith(Sink.ignore)
    }

    def createConsumerSettings(group: String): ConsumerSettings[String, String] = {
      ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
        .withBootstrapServers(bootstrapServers)
        .withGroupId(group)
        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        .withWakeupTimeout(10.seconds)
        .withMaxWakeups(10)
    }
  }
}


