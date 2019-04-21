package com.supersonic.kafka_mirror

import java.net.ServerSocket
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

  case class KafkaConnection(kafka: KafkaServer,
                             zookeeper: ServerCnxnFactory,
                             config: EmbeddedKafkaConfig)

  private var kafkaConnection1: KafkaConnection = _
  private var kafkaConnection2: KafkaConnection = _

  def kafkaHelper1 = new KafkaHelper(kafkaConnection1.config)

  def kafkaHelper2 = new KafkaHelper(kafkaConnection2.config)

  def uuid() = UUID.randomUUID().toString

  def createTopicOnServers(number: Int) = {
    val topic = s"topic$number-" + uuid()

    EmbeddedKafka.createCustomTopic(topic)(kafkaConnection1.config)
    EmbeddedKafka.createCustomTopic(topic)(kafkaConnection2.config)

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
      (_: String).toInt,
      // since partition numbers start from '0', we must subtract here
      (n: Int) => n - 1)

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
    def connectToKafka() = {

      val zkLogsDir = Directory.makeTemp("zookeeper-logs")
      val kafkaLogsDir = Directory.makeTemp("kafka-logs")

      val zookeeper = EmbeddedKafka.startZooKeeper(0, zkLogsDir)

      val config = EmbeddedKafkaConfig(kafkaPort = getFreePort(), zooKeeperPort = zookeeper.getLocalPort)
      val kafka = EmbeddedKafka.startKafka(config, kafkaLogsDir)

      KafkaConnection(kafka, zookeeper, config)
    }

    kafkaConnection1 = connectToKafka()
    kafkaConnection2 = connectToKafka()
  }

  private def shutdownKafka() =
    List(kafkaConnection1, kafkaConnection2).foreach { connection =>
      connection.kafka.shutdown()
      connection.zookeeper.shutdown()
    }

  private def getFreePort() = {
    var socket: ServerSocket = null
    try {
      socket = new ServerSocket(0)
      socket.setReuseAddress(true)
      socket.getLocalPort
    } finally socket.close()
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
        .viaMat(Producer.flexiFlow(producerSettings))(Keep.right)

      source.runWith(Sink.ignore)
    }

    def createConsumerSettings(group: String): ConsumerSettings[String, String] = {
      ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
        .withBootstrapServers(bootstrapServers)
        .withGroupId(group)
        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    }
  }
}


