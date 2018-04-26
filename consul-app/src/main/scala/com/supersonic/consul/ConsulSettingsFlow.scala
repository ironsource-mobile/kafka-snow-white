package com.supersonic.consul

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.supersonic.kafka_mirror.MirrorID

object ConsulSettingsFlow {
  /** Creates a flow that maps raw data coming from a Consul keys into mirror-IDs.
    * The keys are all based at the given root, so that the resulting IDs are all under the root
    * and the root prefix is removed. See [[convertPathsToIDs]].
    */
  def apply(root: String): Flow[Map[String, Option[String]], Map[MirrorID, Option[String]], NotUsed] =
    Flow[Map[String, Option[String]]].map(convertPathsToIDs(root))

  val sep = "/"

  /** Converts hierarchical data coming from a key in Consul into flat data that is keyed
    * by unique IDs. The IDs are all of the keys that are under the given root key.
    *
    * E.g., if the root is 'foo/bar' then a map with:
    * - foo/bar/a -> 1
    * - foo/bar/b -> 2
    * - foo/bar/c -> 3
    *
    * Will be converted to:
    * - a -> 1
    * - b -> 2
    * - c -> 3
    */
  private[consul] def convertPathsToIDs[A](root: String)
                                          (data: Map[String, A]): Map[MirrorID, A] = {
    // fetches the paths that are directly beneath the root.
    def fetchUnderRoot(path: String): Option[List[String]] = {
      val parts = getParts(path)
      val rootParts = getParts(root)

      if (!parts.forall(_.isEmpty) && parts.startsWith(rootParts)) Some {
        parts.drop(rootParts.size)
      } else None
    }

    data.flatMap { case (path, value) =>
      val maybeID = fetchUnderRoot(path).flatMap {
        case List(id) => Some(id)
        case _ => None
      }

      maybeID.map(MirrorID(_) -> value)
    }
  }

  private def getParts(path: String) = path.split(sep).toList.filterNot(_.isEmpty)
}
