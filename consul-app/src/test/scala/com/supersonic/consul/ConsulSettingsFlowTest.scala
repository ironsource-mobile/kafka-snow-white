package com.supersonic.consul

import com.supersonic.consul.ConsulSettingsFlow._
import com.supersonic.kafka_mirror.TestUtil.id
import org.scalatest.{Matchers, WordSpec}

class ConsulSettingsFlowTest extends WordSpec with Matchers {

  "Converting paths to IDs" should {

    "drop the root prefix" in {
      val data = Map(
        "foo/bar/a" -> 1,
        "foo/bar/b" -> 2,
        "foo/bar/c" -> 3)

      val expected = Map(
        id("a") -> 1,
        id("b") -> 2,
        id("c") -> 3)

      convertPathsToIDs("foo/bar")(data) shouldBe expected
    }

    "ignore keys not under the root" in {
      val data = Map(
        "foo/bar/a" -> 1,
        "foo/qux/b" -> 2,
        "baz/c" -> 3)

      val expected = Map(id("a") -> 1)

      convertPathsToIDs("foo/bar")(data) shouldBe expected
    }

    "ignore deeply nested keys under the root" in {
      val data = Map(
        "foo/bar/a/b" -> 1,
        "foo/bar/c/d/e" -> 2)

      convertPathsToIDs("foo/bar")(data) shouldBe empty
    }

    "remove trailing slashes" in {
      val data = Map("foo/bar/a/" -> 1)

      convertPathsToIDs("foo/bar")(data) shouldBe Map(id("a") -> 1)
    }

    "ignore the trailing slash on the root" in {
      val data = Map("foo/bar/a" -> 1)

      convertPathsToIDs("foo/bar/")(data) shouldBe Map(id("a") -> 1)
    }

    "ignore the root key" in {
      val data1 = Map("foo/bar/" -> 1)
      val data2 = Map("foo/bar" -> 1)

      convertPathsToIDs("foo/bar")(data1) shouldBe empty
      convertPathsToIDs("foo/bar")(data2) shouldBe empty
    }
  }
}
