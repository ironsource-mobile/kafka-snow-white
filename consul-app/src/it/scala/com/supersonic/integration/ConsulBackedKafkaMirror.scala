package com.supersonic.integration

import akka.actor.ActorSystem
import akka.stream.testkit.TestSubscriber.Probe
import akka.testkit.TestKit
import com.supersonic.consul.{CancellationToken, ConsulIntegrationSpec, ConsulSettingsFlow}
import com.supersonic.integration.KafkaMirrorIntegrationTemplate.MirrorConfigManager
import com.supersonic.kafka_mirror._
import org.scalatest.Matchers

class ConsulBackedKafkaMirror extends TestKit(ActorSystem("ConsulBackedKafkaMirror"))
                                      with KafkaMirrorIntegrationTemplate
                                      with ConsulIntegrationSpec
                                      with Matchers {

  protected def configSourceName: String = "Consul"

  protected type ConfigMat = CancellationToken

  protected def withProbe[B](transform: MirrorConfigSource => RunningMirrorSource)
                            (f: (Probe[Map[MirrorID, RunningMirror]], MirrorConfigManager) => B): B = {
    val root = "foo"

    val mirrorConfigManager = new MirrorConfigManager {
      def addMirror(name: String, settings: String): Unit = {
        val _ = keyValueClient.putValue(s"$root/$name", settings)
      }

      def deleteMirror(name: String): Unit = keyValueClient.deleteKey(s"$root/$name")
    }


    def withProbe(probe: Probe[Map[MirrorID, RunningMirror]]) = f(probe, mirrorConfigManager)

    withStreamFromConsulProbe(root, consul)(
      source => transform(source.via(ConsulSettingsFlow(root))))(withProbe)
  }

  runKafkaMirrorIntegration()
}
