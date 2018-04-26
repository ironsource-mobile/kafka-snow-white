package com.supersonic.main

import java.net.URL
import akka.event.LoggingAdapter
import com.orbitz.consul.Consul
import com.supersonic.main.KafkaMirrorAppTemplate.BaseAppSettings
import spray.json.RootJsonFormat
import scala.concurrent.duration.FiniteDuration
import net.ceedubs.ficus.readers.ValueReader
import shapeless.Typeable

case class ConsulAppSettings(port: Int,
                             consulSettings: ConsulSettings,
                             stateVerificationInterval: Option[FiniteDuration]) extends BaseAppSettings

case class ConsulSettings(url: URL,
                          rootKey: String,
                          staggerTime: FiniteDuration)

object ConsulSettings {
  def verifySettings(consulSettings: ConsulSettings, consul: Consul)
                    (implicit logger: LoggingAdapter): Unit = {
    val key = consulSettings.rootKey
    val withTrailingSlash = if (key.endsWith("/")) key else s"$key/"
    val result = Option(consul.keyValueClient().getValues(withTrailingSlash))

    if (result.isEmpty)
      logger.warning(s"The provided Consul root key: [$key] is either missing or not a directory")
  }
}

object ConsulAppSettings {
  // the imports below are actually used

  import com.supersonic.util.URLUtil._

  implicit val valueReader = {
    import net.ceedubs.ficus.readers.namemappers.implicits.hyphenCase
    import net.ceedubs.ficus.Ficus._
    import net.ceedubs.ficus.readers.ArbitraryTypeReader._

    implicitly[ValueReader[ConsulAppSettings]]
  }

  implicit val format: RootJsonFormat[ConsulAppSettings] = {
    import com.supersonic.util.DurationUtil._
    import fommil.sjs.FamilyFormats._
    shapeless.cachedImplicit
  }

  // needed to help deriving JSON formats
  val typeable: Typeable[ConsulAppSettings] = implicitly[Typeable[ConsulAppSettings]]
}
