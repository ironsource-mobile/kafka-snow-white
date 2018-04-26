package com.supersonic.file_watch

import java.nio.file.{Files, Path}
import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.google.common.io.{Files => GFiles}
import com.supersonic.kafka_mirror.MirrorID

object ConfigFileSettingsProcessing {
  val confFileSuffix = "conf"

  /** Creates a flow that takes a the contents of a directory and filters it down
    * to only the '.conf' file in it. Each '.conf' file's name becomes a [[MirrorID]].
    *
    * E.g., if the directory contains the following files:
    * - a.conf
    * - b.txt
    * - c.conf
    *
    * Then the resulting mirrors will have the following IDs:
    * - a
    * - c
    */
  def flow: Flow[Map[Path, Option[String]], Map[MirrorID, Option[String]], NotUsed] =
    Flow[Map[Path, Option[String]]].map(convertPathsToIDs)

  private[file_watch] def convertPathsToIDs[A](data: Map[Path, A]): Map[MirrorID, A] = {
    data.flatMap { case (path, value) =>
      val fileName = path.toFile.getName

      val id = if (!Files.isDirectory(path) &&
        confFileSuffix == GFiles.getFileExtension(fileName)) Some {
        GFiles.getNameWithoutExtension(fileName)
      }
      else None

      id.map(MirrorID(_) -> value)
    }
  }
}
