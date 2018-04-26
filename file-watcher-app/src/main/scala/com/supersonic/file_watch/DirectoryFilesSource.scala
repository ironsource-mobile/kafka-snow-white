package com.supersonic.file_watch

import java.nio.file.{Files, Path}
import akka.NotUsed
import akka.event.LoggingAdapter
import akka.stream.alpakka.file.DirectoryChange
import akka.stream.alpakka.file.scaladsl.DirectoryChangesSource
import akka.stream.scaladsl.Source
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

/** Creates a source that listens to changes in a directory and stores the directory's file contents
  * in a [[Map]].
  */
class DirectoryFilesSource(implicit logger: LoggingAdapter) {

  /** Creates a source that listens to changes at the given directory and aggregates the contents
    * of the files in the directory into a map that is keyed by the file's path.
    * The other parameters are as per [[DirectoryChangesSource]]'s documentation.
    */
  def apply(directory: Path,
            pollInterval: FiniteDuration,
            maxBufferSize: Int): Source[Map[Path, Option[String]], NotUsed] = {

    val source =
      DirectoryChangesSource(directory, pollInterval = pollInterval, maxBufferSize = maxBufferSize)

    source.scan(readAllFiles(directory)) { case (acc, (path, change)) =>
      val newAcc = change match {
        case DirectoryChange.Modification =>
          acc.updated(path, readFile(path))

        case DirectoryChange.Creation =>
          acc.updated(path, readFile(path))

        case DirectoryChange.Deletion =>
          acc - path
      }

      newAcc
    }
  }

  private def readAllFiles(directory: Path): Map[Path, Option[String]] = {
    import scala.collection.JavaConverters._

    Files.list(directory)
      .iterator().asScala
      .map(path => path -> readFile(path))
      .toMap
  }

  private def readFile(path: Path): Option[String] = {
    def log(t: Throwable) = {
      logger.error(t, s"Failed reading a file at: [$path]")
      None
    }

    if (!Files.isDirectory(path) && Files.isReadable(path))
      Try(new String(Files.readAllBytes(path)))
        .fold(log, Some(_))
    else None
  }
}
