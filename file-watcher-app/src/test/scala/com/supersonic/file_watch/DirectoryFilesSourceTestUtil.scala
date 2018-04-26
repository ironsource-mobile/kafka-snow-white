package com.supersonic.file_watch

import java.nio.file.Path
import akka.NotUsed
import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.stream.Materializer
import akka.stream.scaladsl.{Keep, Source}
import akka.stream.testkit.TestSubscriber.Probe
import akka.stream.testkit.scaladsl.TestSink
import com.supersonic.file_watch.TestDirectory.FileHelper
import scala.concurrent.duration._

trait DirectoryFilesSourceTestUtil extends TestDirectory {
  def withDirectoryProbe[A](f: (FileHelper, Probe[Map[Path, Option[String]]]) => A)
                           (implicit logger: LoggingAdapter,
                            system: ActorSystem,
                            materializer: Materializer): A =
    withDirectoryProbe[Map[Path, Option[String]], A](identity)(f)

  def withDirectoryProbe[A, B](transform: Source[Map[Path, Option[String]], NotUsed] => Source[A, NotUsed])
                              (f: (FileHelper, Probe[A]) => B)
                              (implicit logger: LoggingAdapter,
                               system: ActorSystem,
                               materializer: Materializer): B = {
    withDirectory { fileHelper =>
      val source = transform {
        new DirectoryFilesSource()(logger)(fileHelper.dir, 5.seconds, 1000)
      }

      val probe = source.toMat(TestSink.probe)(Keep.right).run()

      try f(fileHelper, probe)
      finally {
        val _ = probe.cancel()
      }
    }
  }
}
