package com.supersonic.util

import akka.actor.ActorSystem
import scala.concurrent.Await
import scala.concurrent.duration.Duration

object ActorSystemUtil {
  /** Creates an actor system and runs the given function with it.
    *
    * Terminating the actor system when the function completes (meaning that you shouldn't use
    * Futures in this context as they will be returned almost immediately and the system will be shutdown).
    */
  def withActorSystem[A](name: String)
                        (f: ActorSystem => A): A = {
    val actorSystem = ActorSystem(name)

    try f(actorSystem)
    finally {
      val _ = Await.result(actorSystem.terminate(), Duration.Inf)
    }
  }
}
