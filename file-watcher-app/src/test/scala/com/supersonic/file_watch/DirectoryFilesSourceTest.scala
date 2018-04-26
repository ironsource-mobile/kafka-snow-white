package com.supersonic.file_watch

import akka.actor.ActorSystem
import akka.event.Logging
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import org.scalatest.{Matchers, WordSpecLike}
import scala.concurrent.duration._

class DirectoryFilesSourceTest extends TestKit(ActorSystem("DirectorySettingsProcessingSourceTest"))
                                       with DirectoryFilesSourceTestUtil
                                       with WordSpecLike
                                       with Matchers {
  implicit val materializer = ActorMaterializer()
  implicit val logger = Logging(system.eventStream, "DirectorySettingsProcessingSourceTest")

  "The directory-source" should {
    "listen to file creation" in withDirectoryProbe { (fileHelper, probe) =>
      import fileHelper._

      probe.requestNext() shouldBe empty

      createFile(name = "a", content = "a-content")

      probe.requestNext() shouldBe Map(path("a") -> Some("a-content"))
    }

    "listen to file modification" in withDirectoryProbe { (fileHelper, probe) =>
      import fileHelper._

      probe.requestNext()

      createFile(name = "a", content = "a-content")
      probe.requestNext() shouldBe Map(path("a") -> Some("a-content"))

      modifyFile(name = "a", content = "a-content-2")
      probe.requestNext() shouldBe Map(path("a") -> Some("a-content-2"))
    }

    "listen to file deletion" in withDirectoryProbe { (fileHelper, probe) =>
      import fileHelper._

      probe.requestNext()

      createFile(name = "a", content = "a-content")
      probe.requestNext() shouldBe Map(path("a") -> Some("a-content"))

      deleteFile(name = "a")
      probe.requestNext()
      probe.requestNext() shouldBe empty
    }

    "ignore directories" in withDirectoryProbe { (fileHelper, probe) =>
      import fileHelper._

      probe.requestNext() shouldBe empty

      createDir(name = "a")

      probe.requestNext() shouldBe Map(path("a") -> None)
    }

    "read all files that are present on startup" in {
      withDirectory { fileHelper =>
        import fileHelper._

        createFile(name = "a", content = "a-content")
        createFile(name = "b", content = "b-content")
        createDir(name = "c")

        val source = new DirectoryFilesSource()(logger)(fileHelper.dir, 5.seconds, 1000)

        val probe = source.toMat(TestSink.probe)(Keep.right).run()

        try {
          probe.requestNext() shouldBe Map(
            path("a") -> Some("a-content"),
            path("b") -> Some("b-content"),
            path("c") -> None)

          deleteFile("a")

          probe.requestNext() shouldBe Map(
            path("b") -> Some("b-content"),
            path("c") -> None)
        }
        finally {
          val _ = probe.cancel()
        }
      }
    }

    "mix and match operations" in withDirectoryProbe { (fileHelper, probe) =>
      import fileHelper._

      probe.requestNext() shouldBe empty

      createFile(name = "a", content = "a-content")
      probe.requestNext() shouldBe Map(path("a") -> Some("a-content"))
      probe.requestNext() // for some reason, each creation event is accompanied with a modification event

      createFile(name = "b", content = "b-content")
      probe.requestNext() shouldBe Map(
        path("a") -> Some("a-content"),
        path("b") -> Some("b-content"))
      probe.requestNext()

      createFile(name = "c", content = "c-content")
      probe.requestNext() shouldBe Map(
        path("a") -> Some("a-content"),
        path("b") -> Some("b-content"),
        path("c") -> Some("c-content"))
      probe.requestNext()

      createDir("d")
      probe.requestNext() shouldBe Map(
        path("a") -> Some("a-content"),
        path("b") -> Some("b-content"),
        path("c") -> Some("c-content"),
        path("d") -> None)

      deleteFile(name = "c")
      probe.requestNext() shouldBe Map(
        path("a") -> Some("a-content"),
        path("b") -> Some("b-content"),
        path("d") -> None)

      modifyFile(name = "b", content = "b-content-2")
      probe.requestNext() shouldBe Map(
        path("a") -> Some("a-content"),
        path("b") -> Some("b-content-2"),
        path("d") -> None)

      deleteFile("d")
      probe.requestNext() shouldBe Map(
        path("a") -> Some("a-content"),
        path("b") -> Some("b-content-2"))
    }
  }
}
