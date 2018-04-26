package com.supersonic.util

import cats.data.{NonEmptyList, Validated}
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ValueReader
import scala.util.Try

object ValidatedUtil {
  type ValidOrErrors[A] = Validated[NonEmptyList[Throwable], A]

  implicit class ValidatedOps[E, A](validated: Validated[E, A]) {
    def flatten[B](implicit ev: A <:< Validated[E, B]) = validated.map(ev).andThen(identity)
  }

  implicit def validatedValueReader[A](implicit reader: ValueReader[A]): ValueReader[ValidOrErrors[A]] =
    new ValueReader[ValidOrErrors[A]] {
      override def read(config: Config, path: String) = Validated
        .fromTry(config.as[Try[A]](path))
        .toValidatedNel
    }
}
