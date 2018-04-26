package com.supersonic.file_watch

import java.nio.file.{Files, Path, Paths}
import java.util.Comparator
import com.supersonic.file_watch.TestDirectory._

/** A helper trait that facilitates working with temporary directories for test purposes. */
trait TestDirectory {
  def withDirectory[A](f: FileHelper => A): A = {
    val tempDir = System.getProperty("java.io.tmpdir")
    val dir = Paths.get(tempDir, s"test-folder-${System.nanoTime()}")

    Files.createDirectory(dir)

    try f(new FileHelper(dir))
    finally delete(dir)
  }
}

object TestDirectory {
  class FileHelper(val dir: Path) {
    def path(name: String) = dir.resolve(name)

    def createFile(name: String, content: String): Unit = {
      val _ = Files.write(path(name), content.getBytes)
    }

    def deleteFile(name: String): Unit = delete(path(name))

    def modifyFile(name: String, content: String): Unit =
      createFile(name, content) // creating an existing file just overwrites it

    def createDir(name: String): Unit = {
      val _ = Files.createDirectory(path(name))
    }

    def deleteDir(name: String): Unit = delete(path(name))
  }

  private def delete(path: Path): Unit = {
    import scala.collection.JavaConverters._

    Files.walk(path)
      .sorted(Comparator.reverseOrder())
      .iterator().asScala
      .foreach(Files.delete)
  }
}
