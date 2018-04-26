package com.supersonic.kafka_mirror

import akka.kafka.scaladsl.Consumer.Control

case class MirrorID(value: String)

/** Commands to manage the state of Kafka mirrors in the system. */
sealed trait MirrorCommand extends Product with Serializable

object MirrorCommand {
  /** Indicates that a mirror with the given settings under the specified ID should be started.
    * This can potentially mean that an old mirror with the same ID should be stopped first.
    */
  case class Start(id: MirrorID,
                   mirrorSettings: ExternalKafkaMirrorSettings) extends MirrorCommand

  /** Indicates that the mirror with the given ID should be stopped. */
  case class Stop(id: MirrorID) extends MirrorCommand

  /** Signals that the state of the mirrors should be verified.
    * E.g., check whether some mirrors were terminated unexpectedly.
    */
  case object VerifyState extends MirrorCommand

  case object Shutdown extends MirrorCommand
}

/** Encapsulates the data needed to maintain a running mirror. */
case class RunningMirror(control: Control, mirrorSettings: ExternalKafkaMirrorSettings)

