package com.supersonic.integration

import akka.actor.ActorSystem
import akka.event.Logging
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.stream.testkit.TestSubscriber.Probe
import com.supersonic.integration.KafkaMirrorIntegrationTemplate.MirrorConfigManager
import com.supersonic.kafka_mirror.MirrorCommand.VerifyState
import com.supersonic.kafka_mirror._
import org.scalatest.{AsyncWordSpecLike, Matchers}
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

/** A template class for testing a Kafka mirroring flow. */
trait KafkaMirrorIntegrationTemplate extends AkkaStreamsKafkaIntegrationSpec
                                             with AsyncWordSpecLike
                                             with Matchers {
  protected def configSourceName: String

  /** The type to which the configuration source is materialized. */
  protected type ConfigMat

  protected implicit lazy val logger = Logging(system.eventStream, "KafkaMirrorIntegration")

  private lazy val mirrorActorSystem = ActorSystem("MirrorActorSystem")
  private lazy val mirrorMaker = new SourceBackedMirrorMaker(ActorMaterializer()(mirrorActorSystem), logger)
  private lazy val mirrorManager = new MirrorManager(mirrorMaker)(mirrorActorSystem.dispatcher, logger)

  private def messages1 = 1 to 100

  private def messages2 = 101 to 150

  protected type MirrorConfigSource = Source[Map[MirrorID, Option[String]], ConfigMat]
  protected type RunningMirrorSource = Source[Map[MirrorID, RunningMirror], ConfigMat]

  /** Creates a probe that is used to test the whole flow.
    * The transform takes some input and converts into a Kafka-mirroring source.
    */
  protected def withProbe[B](transform: MirrorConfigSource => RunningMirrorSource)
                            (f: (Probe[Map[MirrorID, RunningMirror]], MirrorConfigManager) => B): B

  private def makeMirrorSource(source: MirrorConfigSource): RunningMirrorSource =
    source.via(SettingsProcessingFlow(system.settings.config))
      .merge(Source.tick(1.second, 1.second, VerifyState))
      .via(mirrorManager.flow)

  protected def runKafkaMirrorIntegration(): Unit = {
    s"A Kafka mirror backed by $configSourceName" should {

      s"start and stop and clean mirrors according to changes in $configSourceName" in {
        val topic1 = createTopicOnServers(1)
        val topic2 = createTopicOnServers(2)

        val group1 = createGroup(1)
        val group2 = createGroup(2)
        val group3 = createGroup(3)
        val group4 = createGroup(4)

        withProbe(makeMirrorSource) { (mirrorsProbe, mirrorConfigManager) =>
          import mirrorConfigManager._

          val waitForID = Waiter(mirrorsProbe)

          produceMessages(topic1, messages1)

          info("starting the 'bar' mirror")
          addMirror("bar", mirrorSettings(topic1, group1))

          info("verifying that 'bar' is active")
          waitForID("bar")
          verifyMirroring(topic1, group2, messages1)

          info("starting the 'qux' mirror")
          addMirror("qux", mirrorSettings(topic2, group3))

          produceMessages(topic2, messages1)

          info("verifying that 'qux' is active")
          waitForID("qux")
          verifyMirroring(topic2, group4, messages1)

          info("stopping bar")
          deleteMirror("bar")
          waitForID("bar", checkPresent = false)

          info("verifying that 'bar' is no longer active")
          verifyNoMirroring(topic1, group2)

          info("shutting down the 'qux' mirror and waiting for it to be automatically restarted")
          waitForID.state(MirrorID("qux")).control.shutdown().map { _ =>
            produceMessages(topic2, messages2)

            info("verifying that 'qux' was restarted")
            verifyMirroring(topic2, group4, messages2)

            succeed
          }
        }
      }
    }
  }

  def verifyMirroring(topic: String, group: String, messages: Range) = Future {
    val topicProbe = createProbe(kafkaHelper2.createConsumerSettings(group), topic)

    topicProbe
      .request(messages.size)
      .expectNextN(messages.map(_.toString))

    topicProbe.cancel()
  }

  def verifyNoMirroring(topic: String, group: String) = {
    Await.result(kafkaHelper1.produce(topic, 200 to 250), remainingOrDefault)

    val targetProbe = createProbe(kafkaHelper2.createConsumerSettings(group), topic)
    targetProbe
      .request(50)
      .expectNoMessage(3.seconds)

    targetProbe.cancel()
  }

  def mirrorSettings(topic: String, group: String) = s"""
    consumer = {
       kafka-clients = {
         bootstrap.servers = "${kafkaHelper1.bootstrapServers}"
         group.id = "$group"
         auto.offset.reset = "earliest"
       }
    }

    producer = {
      kafka-clients = {
       bootstrap.servers = "${kafkaHelper2.bootstrapServers}"
     }
    }

    mirror = {
      whitelist = [$topic]
      commitBatchSize = 5
      commitParallelism = 4
    }
  """

  case class Waiter(mirrorsProbe: Probe[Map[MirrorID, RunningMirror]]) {
    @volatile var state: Map[MirrorID, RunningMirror] = Map.empty

    def apply(id: String, checkPresent: Boolean = true) = {
      var attempts = 50

      def check() = {
        val mirrorPresent = state.contains(MirrorID(id))

        if (checkPresent) mirrorPresent
        else !mirrorPresent
      }

      while (!check() && attempts > 0) {
        state = mirrorsProbe.requestNext(1.minute)
        attempts -= 1
      }

      if (attempts == 0) fail(s"Failed while waiting for the [$id] mirror ID")
    }
  }
}

object KafkaMirrorIntegrationTemplate {
  /** A trait that allows creating and removing mirror configurations. */
  trait MirrorConfigManager {

    /** Creates a new mirror with the given settings. */
    def addMirror(name: String, settings: String): Unit

    /** Deletes a mirror with the given name. */
    def deleteMirror(name: String): Unit
  }
}
