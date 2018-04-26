package com.supersonic.integration

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.testkit.TestSubscriber.Probe
import akka.testkit.TestKit
import com.supersonic.file_watch.{ConfigFileSettingsProcessing, DirectoryFilesSourceTestUtil}
import com.supersonic.integration.KafkaMirrorIntegrationTemplate.MirrorConfigManager
import com.supersonic.kafka_mirror.{MirrorID, RunningMirror}
import org.scalatest.Matchers

class DirectoryBackedKafkaMirrorTest extends TestKit(ActorSystem("DirectoryBackedKafkaMirrorTest"))
                                             with KafkaMirrorIntegrationTemplate
                                             with DirectoryFilesSourceTestUtil
                                             with Matchers {
  protected def configSourceName: String = "a directory"

  protected type ConfigMat = NotUsed

  protected def withProbe[B](transform: MirrorConfigSource => RunningMirrorSource)
                            (f: (Probe[Map[MirrorID, RunningMirror]], MirrorConfigManager) => B): B = {
    withDirectoryProbe(source => transform(source.via(ConfigFileSettingsProcessing.flow))) { (fileHelper, probe) =>
      val mirrorConfigManager = new MirrorConfigManager {
        def addMirror(name: String, settings: String) = fileHelper.createFile(s"$name.conf", settings)

        def deleteMirror(name: String) = fileHelper.deleteFile(s"$name.conf")
      }

      f(probe, mirrorConfigManager)
    }
  }

  runKafkaMirrorIntegration()
}
