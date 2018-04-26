package com.supersonic.main

import java.nio.file.{Files, Path, Paths}
import akka.event.LoggingAdapter
import com.supersonic.main.KafkaMirrorAppTemplate.BaseAppSettings
import spray.json.RootJsonFormat
import scala.concurrent.duration.FiniteDuration
import net.ceedubs.ficus.readers.ValueReader
import shapeless.Typeable
import scala.util.Try

case class DirectoryAppSettings(port: Int,
                                mirrorsDirectory: String,
                                pollInterval: Option[FiniteDuration],
                                maxBufferSize: Option[Int],
                                stateVerificationInterval: Option[FiniteDuration]) extends BaseAppSettings {
  def rootKey: String = mirrorsDirectory

  /** Tries to convert the mirrors directory into a valid path. Throws an exception and
    * adds logging if it is not.
    */
  def getMirrorDirectoryPath(implicit logger: LoggingAdapter): Path = {
    val maybePath = Try(Paths.get(mirrorsDirectory))

    def logException(throwable: Throwable) = {
      logger.error(throwable, s"Failed to convert [$mirrorsDirectory] into a valid path")
      throw throwable
    }

    val path = maybePath.fold(logException, identity)

    if (Files.isDirectory(path)) path
    else sys.error(s"The provided path is not a directory: [$mirrorsDirectory]")
  }
}


object DirectoryAppSettings {
  // the imports below are actually used

  implicit val valueReader = {
    import net.ceedubs.ficus.readers.namemappers.implicits.hyphenCase
    import net.ceedubs.ficus.Ficus._
    import net.ceedubs.ficus.readers.ArbitraryTypeReader._

    implicitly[ValueReader[DirectoryAppSettings]]
  }

  implicit val format: RootJsonFormat[DirectoryAppSettings] = {
    import com.supersonic.util.DurationUtil._
    import fommil.sjs.FamilyFormats._

    shapeless.cachedImplicit
  }

  // needed to help deriving JSON formats
  val typeable: Typeable[DirectoryAppSettings] = implicitly[Typeable[DirectoryAppSettings]]
}
