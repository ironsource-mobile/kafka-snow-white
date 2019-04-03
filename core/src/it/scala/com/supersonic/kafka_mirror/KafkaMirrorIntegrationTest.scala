package com.supersonic.kafka_mirror

import akka.actor.ActorSystem
import akka.event.Logging
import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import org.scalatest.{Matchers, WordSpecLike}
import scala.concurrent.Await
import concurrent.duration._

class KafkaMirrorIntegrationTest extends TestKit(ActorSystem("KafkaMirrorIntegrationTest"))
                                         with AkkaStreamsKafkaIntegrationSpec
                                         with WordSpecLike
                                         with Matchers {

  val logger = Logging(system.eventStream, "KafkaMirrorIntegrationTest")

  val kafkaMirror = KafkaMirror[String, String](new SourceBackedMirrorMaker(materializer, logger)) _

  "The Kafka mirror" should {
    val group1 = createGroup(1)
    val group2 = createGroup(2)

    def makeMirror(whitelist: String*) =
      kafkaMirror(MirrorID("test"), mirrorSettings(group1)(whitelist: _*)).source

    val messages1 = 1 to 100
    val messages2 = 101 to 200

    "move messages between topics in different Kafka servers using the mirror" in {
      val topic1 = createTopicOnServers(1)
      val topic2 = createTopicOnServers(2)

      produceMessages(topic1, messages1)
      produceMessages(topic2, messages2)

      val mirror = makeMirror(topic1, topic2)

      val pullingProbe = mirror.runWith(TestSink.probe)

      pullingProbe
        .request(messages1.size + messages2.size) // should request all messages, even if batching is set to 1
        .expectNext(10.seconds) // we can't expect as many messages, since batching can compact many messages into one

      verifyTopic(topic1, group2, messages1)
      verifyTopic(topic2, group2, messages2)

      pullingProbe.cancel()
    }

    "commit messages in the source topic upon mirroring" in {
      val topic = createTopicOnServers(1)

      produceMessages(topic, messages1)

      val mirror = makeMirror(topic)

      val (control, pullingProbe) = mirror.toMat(TestSink.probe)(Keep.both).run()

      pullingProbe // requesting the first batch
        .request(1)
        .expectNext(10.seconds)

      pullingProbe.cancel()
      Await.result(control.isShutdown, 1.minute)

      // continuing consumption within the same group,
      // should skip messages from the first batch since it was already committed
      val sourceTopicProbe = createProbe(kafkaHelper1.createConsumerSettings(group1), topic)
      val element = sourceTopicProbe.request(1).expectNext(10.seconds)

      element.toInt should be > messages1.head

      sourceTopicProbe.cancel()
    }

    "support message bucketing" in {
      val bucketing = BucketSettings(
        mirrorBuckets = 3,
        totalBuckets = 5
      )

      val topic = createTopicOnServers(1)

      produceMessages(topic, messages1)

      val mirror = kafkaMirror(MirrorID("test"), mirrorSettings(group1, Some(bucketing))(topic)).source

      val pullingProbe = mirror.runWith(TestSink.probe)

      pullingProbe
        .request(messages1.size) // should request all messages, even if batching is set to 1
        .expectNext(10.seconds) // we can't expect as many messages, since batching can compact many messages into one

      val mirroredMessages = messages1.filter(_ % 5 < 3)

      verifyTopic(topic, group1, mirroredMessages)

      pullingProbe.cancel()
    }
  }
}
