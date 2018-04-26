package com.supersonic.file_watch

import java.nio.file.Paths
import com.supersonic.file_watch.ConfigFileSettingsProcessing._
import com.supersonic.kafka_mirror.TestUtil.id
import org.scalatest.{Matchers, WordSpec}

class ConfigFileSettingsProcessingTest extends WordSpec with Matchers {

  "Converting paths to IDs" should {
    def path(name: String) = Paths.get("/a/b", name)

    "use the file-names of '.conf' files" in {
      val data = Map(
        path("a.conf") -> 1,
        path("b.conf") -> 2,
        path("c.conf") -> 3)

      val expected = Map(
        id("a") -> 1,
        id("b") -> 2,
        id("c") -> 3)

      convertPathsToIDs(data) shouldBe expected
    }

    "ignore directories" in {
      val data = Map(
        path("a.conf") -> 1,
        path("b/") -> 2,
        path("c.conf") -> 3)

      val expected = Map(
        id("a") -> 1,
        id("c") -> 3)

      convertPathsToIDs(data) shouldBe expected
    }

    "ignore non-conf files" in {
      val data = Map(
        path("a.conf") -> 1,
        path("b.txt") -> 2,
        path("c.conf") -> 3,
        path("d.txt") -> 4)

      val expected = Map(
        id("a") -> 1,
        id("c") -> 3)

      convertPathsToIDs(data) shouldBe expected
    }
  }
}
