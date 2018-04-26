package com.supersonic.util

import spray.json._
import scala.concurrent.duration.{Duration, FiniteDuration}

object DurationUtil {

  implicit object FiniteDurationFormat extends JsonFormat[FiniteDuration] {
    def write(fd: FiniteDuration) = JsString(fd.toString)

    def read(json: JsValue) = {
      def error() = deserializationError(s"Expected a finite duration but got: $json")

      json match {
        case JsString(duration) => Duration(duration) match {
          case finite: FiniteDuration => finite
          case _ => error()
        }
        case _ => error()
      }
    }
  }
}
