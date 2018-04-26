package com.supersonic.main

import com.supersonic.kafka_mirror.{ExternalKafkaMirrorSettings, MirrorID}
import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import fommil.sjs.FamilyFormats
import shapeless.Typeable
import spray.json._

case class AppState[AppSettings](commitHash: String,
                                 settings: AppSettings,
                                 mirrors: Either[String, List[(MirrorID, ExternalKafkaMirrorSettings)]])

object AppState extends DefaultJsonProtocol with FamilyFormats {
  // boilerplate because of bugs in implicit resolution
  // see here: https://github.com/fommil/spray-json-shapeless/issues/22
  implicit def either[A: JsonFormat, B: JsonFormat] =
    DefaultJsonProtocol.eitherFormat[A, B]

  implicit val configFormat = new JsonFormat[Config] {
    def write(config: Config): JsValue = JsonParser(config.root.render(ConfigRenderOptions.concise))

    def read(json: JsValue): Config = ConfigFactory.parseString(json.toString)
  }

  def format[AppSettings: JsonFormat: Typeable]: RootJsonFormat[AppState[AppSettings]] =
    implicitly[RootJsonFormat[AppState[AppSettings]]]
}
