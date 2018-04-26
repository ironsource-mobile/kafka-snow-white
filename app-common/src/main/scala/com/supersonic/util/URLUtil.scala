package com.supersonic.util

import java.net.URL
import com.typesafe.config.Config
import net.ceedubs.ficus.readers.ValueReader
import spray.json._

object URLUtil {

  implicit object UrlValueReader extends ValueReader[URL] {
    def read(config: Config, path: String) = {
      new URL(config.getString(path))
    }
  }

  implicit object URLFormat extends JsonFormat[URL] {
    def write(obj: URL) = JsString(obj.toString)

    def read(json: JsValue) = json match {
      case JsString(url) => new URL(url)
      case other => deserializationError(s"Expected URL, got: $other")
    }
  }
}
