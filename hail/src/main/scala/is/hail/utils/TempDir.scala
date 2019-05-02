package is.hail.utils

import java.io.IOException

import is.hail.io.fs.FS
import org.apache.hadoop

import scala.util.Random

object TempDir {
  def createTempDir(tmpdir: String, sfs: is.hail.io.fs.FS): String = {
    while (true) {
      try {
        val dir = tmpdir + "/hail." + Random.alphanumeric.take(12).mkString

        if (sfs.exists(dir)) {
          // try again
        } else {
          sfs.mkDir(dir)

          val fs = sfs.fileSystem(tmpdir)
          val qDir = fs.makeQualified(dir)
          fs.deleteOnExit(qDir)

          return qDir.toString
        }
      } catch {
        case e: IOException =>
        // try again
      }
    }

    // can't happen
    null
  }

  def apply(fs: FS): TempDir =
    new TempDir(createTempDir("file:///tmp", fs),
      createTempDir("/tmp", fs))
}

class TempDir(localTempDir: String, tempDir: String) {
  var counter: Int = 0

  def createTempFile(tempDir: String, prefix: String, extension: String): String = {
    val i = counter
    counter += 1

    val sb = new StringBuilder
    sb.append(prefix)
    if (prefix.nonEmpty)
      sb += '.'
    sb.append("%05d".format(i))
    if (extension.nonEmpty)
      sb += '.'
    sb.append(extension)

    tempDir + "/" + sb.result()
  }

  def createTempFile(prefix: String = "", extension: String = ""): String =
    createTempFile(tempDir, prefix, extension)

  def createLocalTempFile(prefix: String = "", extension: String = ""): String =
    createTempFile(localTempDir, prefix, extension)
}
