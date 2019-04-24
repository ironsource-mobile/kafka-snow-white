package com.supersonic.kafka_mirror

import akka.Done
import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.kafka.scaladsl.Consumer.Control
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Keep, Source}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.stream.testkit.{TestPublisher, TestSubscriber}
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.common.{Metric, MetricName}
import scala.concurrent.{ExecutionContext, Future, Promise}

object TestUtil {

  def id(str: String) = MirrorID(str)

  def settings(str: String) = {
    def conf(str: String, i: Int) = ConfigFactory.parseString(s"$str = $i")

    ExternalKafkaMirrorSettings(ExternalKafkaSettings(conf(str, 1), conf(str, 2)),
      MirrorSettings(Set(s"$str-topic"), 1, 2))
  }

  def runnableSettings(str: String)
                      (implicit system: ActorSystem) = {
    val systemConfig = system.settings.config
    def conf(str: String, i: Int) = ConfigFactory.parseString(s"""
       $str = $i
       kafka-clients = {
         bootstrap.servers = "localhost:9092"
       }
    """)
      .withFallback(systemConfig.getConfig("akka.kafka.consumer"))
      .withFallback(systemConfig.getConfig("akka.kafka.producer"))

    ExternalKafkaMirrorSettings(ExternalKafkaSettings(conf(str, 1), conf(str, 2)),
      MirrorSettings(Set(s"$str-topic"), 1, 2))
  }

  def probes[A, B](flow: Flow[A, B, _])
                  (implicit system: ActorSystem,
                   materializer: Materializer): (TestPublisher.Probe[A], TestSubscriber.Probe[B]) =
    TestSource.probe[A]
      .via(flow)
      .toMat(TestSink.probe[B])(Keep.both)
      .run()

  class MockMirrorMaker(create: => KafkaMirror) extends MirrorMaker {
    type Mirror = KafkaMirror

    def apply(mirrorID: MirrorID, producer: Producer[_, _])
             (source: LoggingAdapter => Source[Done, Control]) = {
      // since we don't want it to actually run in the test and it will
      // not be shutdown otherwise (since we are using a mock mirror)
      producer.close()

      create
    }
  }

  class MockKafkaMirror(implicit executionContext: ExecutionContext) extends KafkaMirror {
    @volatile var control = Option.empty[MockControl]

    def start() = {
      val c = new MockControl
      control = Some(c)

      c
    }

    def isStarted() = control.isDefined
  }

  object FailingKafkaMirror extends KafkaMirror {
    def start() = sys.error("mirror failed")
  }

  class MockControl(implicit executionContext: ExecutionContext) extends Control {
    private val shutDownState = Promise[Done]()

    def shutdownNow(): Unit = {
      val _ = shutDownState.trySuccess(Done)
    }

    def isShutdownNow(): Boolean = shutDownState.isCompleted

    override def shutdown() = Future {
      shutdownNow()
      Done
    }

    override def stop() = shutdown()

    override val isShutdown = shutDownState.future

    override def metrics: Future[Map[MetricName, Metric]] = Future.successful(Map.empty)
  }

  object FailingControl extends Control {
    override def shutdown() = Future.failed(new Exception("failed to shutdown"))

    override def stop() = shutdown()

    override def isShutdown = stop()

    override def metrics: Future[Map[MetricName, Metric]] = Future.successful(Map.empty)
  }
}
