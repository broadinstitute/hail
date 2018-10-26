package is.hail.io.tabix

import is.hail.HailContext
import is.hail.utils.{ArrayBuilder, fatal}

import htsjdk.tribble.util.{ParsingUtils, TabixUtils}
import org.apache.hadoop.io.compress.SplitCompressionInputStream

import java.io.InputStream
import java.nio.{ByteBuffer, ByteOrder}

import scala.collection.mutable.HashMap
import scala.language.implicitConversions

// Tabix file format is described here: https://samtools.github.io/hts-specs/tabix.pdf

object TabixReader {
  val MaxBin: Int = 37450
  val TadLidxShift: Int = 14
  val DefaultBufferSize: Int = 1000
  val Magic: Array[Byte] = Array(84, 66, 73, 1) // "TBI\1"

  def readInt(is: InputStream): Int = {
    val buf = new Array[Byte](4)
    is.read(buf)
    ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN).getInt
  }

  def readLong(is: InputStream): Long = {
    val buf = new Array[Byte](8)
    is.read(buf)
    ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN).getLong
  }
}

class TabixReader(val filePath: String, private val idxFilePath: Option[String]) {
  import TabixReader._

  val indexPath: String = idxFilePath match {
    case None => ParsingUtils.appendToPath(filePath, TabixUtils.STANDARD_INDEX_EXTENSION)
    case Some(s) => {
      if (s.endsWith(".tbi"))
        s
      else
        fatal(s"unknown file extension for tabix index: ${s}")
    }
  }

  private val hc = HailContext.get
  private val sc = hc.sc
  private val hConf = sc.hadoopConfiguration
  private lazy val indexStream: SplitCompressionInputStream = null

  lazy val index: Tabix = {
    val is = indexStream
    if (is == null) {
      null
    } else {
      var buf = new Array[Byte](4)
      is.read(buf, 0, 4)
      assert(Magic sameElements buf, "magic number failed validation")
      val seqs = new Array[String](readInt(is))
      val format = readInt(is)
      val colSeq = readInt(is)
      val colBeg = readInt(is)
      val colEnd = readInt(is)
      val meta = readInt(is)
      readInt(is) // unused

      // read the sequence dictionary
      val l = readInt(is) // get the length of
      buf = new Array[Byte](l)
      var (i, j, k) = (0, 0, 0)
      is.read(buf)
      while (i < buf.length) {
        if (buf(i) == 0) {
          val contig = new String(buf.slice(j, i))
          // TODO map????
          seqs(k) = contig
          k += 1
          j = i + 1
        }
        i += 1
      }

      // read the index
      val indices = new ArrayBuilder[(HashMap[Int, Array[TbiPair]], Array[Long])](seqs.length)
      i = 0
      while (i < seqs.length) {
        // binning index
        val nBin = readInt(is);
        val binIdx = new HashMap[Int, Array[TbiPair]]()
        j = 0
        while (j < nBin) {
          val bin = readInt(is);
          val chunks = new Array[TbiPair](readInt(is))
          k = 0
          while (k < chunks.length) {
            chunks(k) = readLong(is) -> readLong(is)
            k += 1
          }
          binIdx += bin -> chunks
          j += 1
        }
        // linear index
        val linIdx = new Array[Long](readInt(is))
        k = 0
        while (k < linIdx.length) {
          linIdx(k) = readLong(is)
          k += 1
        }
        indices += binIdx -> linIdx
        i += 1
      }
      is.close()
      new Tabix(format, colSeq, colBeg, meta, seqs, indices.result())
    }
  }
}

class Tabix(
  val format: Int,
  val colSeq: Int,
  val colBeg: Int,
  val meta: Int,
  val seqs: Array[String],
  val indicies: Array[(HashMap[Int, Array[TbiPair]], Array[Long])]
)

case class TbiPair(_1: Long, _2: Long)

object TbiPair {
  implicit def tup2Pair(t: (Long, Long)): TbiPair = new TbiPair(t._1, t._2)
}

object TbiPairOrd extends Ordering[TbiPair] {
  def compare(t: TbiPair, u: TbiPair) = if (t._1 == u._1) {
    0
  } else if ((t._1 < u._1) ^ (t._1 < 0) ^ (u._1 < 0)) {
    -1
  } else {
    1
  }
}
