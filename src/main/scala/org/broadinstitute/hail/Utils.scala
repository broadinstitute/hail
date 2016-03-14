package org.broadinstitute.hail

import java.io._
import breeze.linalg.operators.{OpSub, OpAdd}
import org.apache.{spark, hadoop}
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.io.IOUtils._
import org.apache.hadoop.io.{BytesWritable, Text, NullWritable}
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.spark.{SparkEnv, AccumulableParam}
import org.apache.spark.mllib.linalg.distributed.IndexedRow
import org.apache.spark.rdd.RDD
import org.broadinstitute.hail.io.hadoop.{BytesOnlyWritable, ByteArrayOutputFormat}
import org.broadinstitute.hail.check.Gen
import org.broadinstitute.hail.io.compress.BGzipCodec
import org.broadinstitute.hail.driver.HailConfiguration
import org.broadinstitute.hail.variant.Variant
import scala.collection.{TraversableOnce, mutable}
import scala.language.implicitConversions
import breeze.linalg.{Vector => BVector, DenseVector => BDenseVector, SparseVector => BSparseVector}
import org.apache.spark.mllib.linalg.{Vector => SVector, DenseVector => SDenseVector, SparseVector => SSparseVector}
import scala.reflect.ClassTag
import org.slf4j.{Logger, LoggerFactory}

import Utils._

final class ByteIterator(val a: Array[Byte]) {
  var i: Int = 0

  def hasNext: Boolean = i < a.length

  def next(): Byte = {
    val r = a(i)
    i += 1
    r
  }

  def readULEB128(): Int = {
    var b: Byte = next()
    var x: Int = b & 0x7f
    var shift: Int = 7
    while ((b & 0x80) != 0) {
      b = next()
      x |= ((b & 0x7f) << shift)
      shift += 7
    }

    x
  }

  def readSLEB128(): Int = {
    var b: Byte = next()
    var x: Int = b & 0x7f
    var shift: Int = 7
    while ((b & 0x80) != 0) {
      b = next()
      x |= ((b & 0x7f) << shift)
      shift += 7
    }

    // sign extend
    if (shift < 32
      && (b & 0x40) != 0)
      x = (x << (32 - shift)) >> (32 - shift)

    x
  }
}

class RichIterable[T](val i: Iterable[T]) extends Serializable {
  def lazyMap[S](f: (T) => S): Iterable[S] = new Iterable[S] with Serializable {
    def iterator: Iterator[S] = new Iterator[S] {
      val it: Iterator[T] = i.iterator

      def hasNext: Boolean = it.hasNext

      def next(): S = f(it.next())
    }
  }

  def foreachBetween(f: (T) => Unit)(g: (Unit) => Unit) {
    var first = true
    i.foreach { elem =>
      if (first)
        first = false
      else
        g()
      f(elem)
    }
  }

  def lazyMapWith[T2, S](i2: Iterable[T2], f: (T, T2) => S): Iterable[S] =
    new Iterable[S] with Serializable {
      def iterator: Iterator[S] = new Iterator[S] {
        val it: Iterator[T] = i.iterator
        val it2: Iterator[T2] = i2.iterator

        def hasNext: Boolean = it.hasNext && it2.hasNext

        def next(): S = f(it.next(), it2.next())
      }
    }

  def lazyFilterWith[T2](i2: Iterable[T2], p: (T, T2) => Boolean): Iterable[T] =
    new Iterable[T] with Serializable {
      def iterator: Iterator[T] = new Iterator[T] {
        val it: Iterator[T] = i.iterator
        val it2: Iterator[T2] = i2.iterator

        var pending: Boolean = false
        var pendingNext: T = _

        def hasNext: Boolean = {
          while (!pending && it.hasNext && it2.hasNext) {
            val n = it.next()
            val n2 = it2.next()
            if (p(n, n2)) {
              pending = true
              pendingNext = n
            }
          }
          pending
        }

        def next(): T = {
          assert(pending)
          pending = false
          pendingNext
        }
      }
    }

  def lazyFlatMap[S](f: (T) => TraversableOnce[S]): Iterable[S] =
    new Iterable[S] with Serializable {
      def iterator: Iterator[S] = new Iterator[S] {
        val it: Iterator[T] = i.iterator
        var current: Iterator[S] = Iterator.empty

        def hasNext: Boolean =
          if (current.hasNext)
            true
          else {
            if (it.hasNext) {
              current = f(it.next()).toIterator
              hasNext
            } else
              false
          }

        def next(): S = current.next()
      }
    }

  def lazyFlatMapWith[S, T2](i2: Iterable[T2], f: (T, T2) => TraversableOnce[S]): Iterable[S] =
    new Iterable[S] with Serializable {
      def iterator: Iterator[S] = new Iterator[S] {
        val it: Iterator[T] = i.iterator
        val it2: Iterator[T2] = i2.iterator
        var current: Iterator[S] = Iterator.empty

        def hasNext: Boolean =
          if (current.hasNext)
            true
          else {
            if (it.hasNext && it2.hasNext) {
              current = f(it.next(), it2.next()).toIterator
              hasNext
            } else
              false
          }

        def next(): S = current.next()
      }
    }

  def areDistinct(): Boolean = {
    val seen = mutable.HashSet[T]()
    for (x <- i)
      if (seen(x))
        return false
      else
        seen += x
    true
  }

  def duplicates(): Set[T] = {
    val dups = mutable.HashSet[T]()
    val seen = mutable.HashSet[T]()
    for (x <- i)
      if (seen(x))
        dups += x
      else
        seen += x
    dups.toSet
  }
}

class RichArrayBuilderOfByte(val b: mutable.ArrayBuilder[Byte]) extends AnyVal {
  def writeULEB128(x0: Int) {
    require(x0 >= 0)

    var x = x0
    var more = true
    while (more) {
      var c = x & 0x7F
      x = x >>> 7

      if (x == 0)
        more = false
      else
        c = c | 0x80

      assert(c >= 0 && c <= 255)
      b += c.toByte
    }
  }

  def writeSLEB128(x0: Int) {
    var more = true
    var x = x0
    while (more) {
      var c = x & 0x7f
      x >>= 7

      if ((x == 0
        && (c & 0x40) == 0)
        || (x == -1
        && (c & 0x40) == 0x40))
        more = false
      else
        c |= 0x80

      b += c.toByte
    }
  }
}

class RichIteratorOfByte(val i: Iterator[Byte]) extends AnyVal {
  /*
  def readULEB128(): Int = {
    var x: Int = 0
    var shift: Int = 0
    var b: Byte = 0
    do {
      b = i.next()
      x = x | ((b & 0x7f) << shift)
      shift += 7
    } while ((b & 0x80) != 0)

    x
  }

  def readSLEB128(): Int = {
    var shift: Int = 0
    var x: Int = 0
    var b: Byte = 0
    do {
      b = i.next()
      x |= ((b & 0x7f) << shift)
      shift += 7
    } while ((b & 0x80) != 0)

    // sign extend
    if (shift < 32
      && (b & 0x40) != 0)
      x = (x << (32 - shift)) >> (32 - shift)

    x
  }
  */
}

// FIXME AnyVal in Scala 2.11
class RichArray[T](a: Array[T]) {
  def index: Map[T, Int] = a.zipWithIndex.toMap

  def areDistinct(): Boolean = a.toIterable.areDistinct()

  def duplicates(): Set[T] = a.toIterable.duplicates()
}

class RichRDD[T](val r: RDD[T]) extends AnyVal {
  def countByValueRDD()(implicit tct: ClassTag[T]): RDD[(T, Int)] = r.map((_, 1)).reduceByKey(_ + _)

  def writeTable(filename: String, header: Option[String] = None, deleteTmpFiles: Boolean = true) {
    val hConf = r.sparkContext.hadoopConfiguration
    val tmpFileName = hadoopGetTemporaryFile(HailConfiguration.tmpDir, hConf)
    val codecFactory = new CompressionCodecFactory(hConf)
    val codec = Option(codecFactory.getCodec(new hadoop.fs.Path(filename)))
    val headerExt = codec.map(_.getDefaultExtension).getOrElse("")

    header.foreach { str =>
      writeTextFile(tmpFileName + ".header" + headerExt, r.sparkContext.hadoopConfiguration) { s =>
        s.write(str)
        s.write("\n")
      }
    }

    codec match {
      case Some(x) => r.saveAsTextFile(tmpFileName, x.getClass)
      case None => r.saveAsTextFile(tmpFileName)
    }

    val filesToMerge = header match {
      case Some(_) => Array(tmpFileName + ".header" + headerExt, tmpFileName + "/part-*")
      case None => Array(tmpFileName + "/part-*")
    }

    hadoopDelete(filename, hConf, recursive = true) // overwriting by default

    val (_, dt) = time {
      hadoopCopyMerge(filesToMerge, filename, hConf, deleteTmpFiles)
    }
    info(s"while writing:\n    $filename\n  merge time: ${formatTime(dt)}")

    if (deleteTmpFiles) {
      hadoopDelete(tmpFileName + ".header" + headerExt, hConf, recursive = false)
      hadoopDelete(tmpFileName, hConf, recursive = true)
    }
  }
}

class RichRDDByteArray(val r: RDD[Array[Byte]]) extends AnyVal {
  def saveFromByteArrays(filename: String, header: Option[Array[Byte]] = None, deleteTmpFiles: Boolean = true) {
    val nullWritableClassTag = implicitly[ClassTag[NullWritable]]
    val bytesClassTag = implicitly[ClassTag[BytesOnlyWritable]]
    val hConf = r.sparkContext.hadoopConfiguration

    val tmpFileName = hadoopGetTemporaryFile(HailConfiguration.tmpDir, hConf)

    header.foreach { str =>
      writeFile(tmpFileName + ".header", r.sparkContext.hadoopConfiguration) { s =>
        s.write(str)
      }
    }

    val filesToMerge = header match {
      case Some(_) => Array(tmpFileName + ".header", tmpFileName + "/part-*")
      case None => Array(tmpFileName + "/part-*")
    }

    val rMapped = r.mapPartitions { iter =>
      val bw = new BytesOnlyWritable()
      iter.map { bb =>
        bw.set(new BytesWritable(bb))
        (NullWritable.get(), bw)
      }
    }

    RDD.rddToPairRDDFunctions(rMapped)(nullWritableClassTag, bytesClassTag, null)
      .saveAsHadoopFile[ByteArrayOutputFormat](tmpFileName)

    hadoopDelete(filename, hConf, recursive = true) // overwriting by default

    val (_, dt) = time {
      hadoopCopyMerge(filesToMerge, filename, hConf, deleteTmpFiles)
    }
    println("merge time: " + formatTime(dt))

    if (deleteTmpFiles) {
      hadoopDelete(tmpFileName + ".header", hConf, recursive = false)
      hadoopDelete(tmpFileName, hConf, recursive = true)
    }
  }
}

class RichIndexedRow(val r: IndexedRow) extends AnyVal {

  def -(that: BVector[Double]): IndexedRow = new IndexedRow(r.index, r.vector - that)

  def +(that: BVector[Double]): IndexedRow = new IndexedRow(r.index, r.vector + that)

  def :/(that: BVector[Double]): IndexedRow = new IndexedRow(r.index, r.vector :/ that)
}

class RichEnumeration[T <: Enumeration](val e: T) extends AnyVal {
  def withNameOption(name: String): Option[T#Value] =
    e.values.find(_.toString == name)
}

class RichMutableMap[K, V](val m: mutable.Map[K, V]) extends AnyVal {
  def updateValue(k: K, default: V, f: (V) => V) {
    m += ((k, f(m.getOrElse(k, default))))
  }
}

class RichMap[K, V](val m: Map[K, V]) extends AnyVal {
  def mapValuesWithKeys[T](f: (K, V) => T): Map[K, T] = m map { case (k, v) => (k, f(k, v)) }

  def force = m.map(identity) // needed to make serializable: https://issues.scala-lang.org/browse/SI-7005
}

class RichOption[T](val o: Option[T]) extends AnyVal {
  def contains(v: T): Boolean = o.isDefined && o.get == v
}

class RichStringBuilder(val sb: mutable.StringBuilder) extends AnyVal {
  def tsvAppend(a: Any) {
    a match {
      case null | None => sb.append("NA")
      case Some(x) => tsvAppend(x)
      case d: Double => sb.append(d.formatted("%.4e"))
      case v: Variant =>
        sb.append(v.contig)
        sb += ':'
        sb.append(v.start)
        sb += ':'
        sb.append(v.ref)
        sb += ':'
        sb.append(v.alt)
      case i: Iterable[_] =>
        var first = true
        i.foreach { x =>
          if (first)
            first = false
          else
            sb += ','
          tsvAppend(x)
        }
      case arr: Array[_] =>
        var first = true
        arr.foreach { x =>
          if (first)
            first = false
          else
            sb += ','
          tsvAppend(x)
        }
      case _ => sb.append(a)
    }
  }

  def tsvAppendItems(args: Any*) {
    args.foreachBetween(tsvAppend) { _ => sb += '\t' }
  }
}

class RichIntPairTraversableOnce[V](val t: TraversableOnce[(Int, V)]) extends AnyVal {
  def reduceByKeyToArray(n: Int, zero: => V)(f: (V, V) => V)(implicit vct: ClassTag[V]): Array[V] = {
    val a = Array.fill[V](n)(zero)
    t.foreach { case (k, v) =>
      a(k) = f(a(k), v)
    }
    a
  }
}

class RichPairTraversableOnce[K, V](val t: TraversableOnce[(K, V)]) extends AnyVal {
  def reduceByKey(f: (V, V) => V): scala.collection.Map[K, V] = {
    val m = mutable.Map.empty[K, V]
    t.foreach { case (k, v) =>
      m.get(k) match {
        case Some(v2) => m += k -> f(v, v2)
        case None => m += k -> v
      }
    }
    m
  }
}

class RichIterator[T](val it: Iterator[T]) extends AnyVal {
  def existsExactly1(p: (T) => Boolean): Boolean = {
    var n: Int = 0
    while (it.hasNext)
      if (p(it.next())) {
        n += 1
        if (n > 1)
          return false
      }
    n == 1
  }
}

class RichBoolean(val b: Boolean) extends AnyVal {
  def ==>(that: => Boolean): Boolean = !b || that

  def iff(that: Boolean): Boolean = b == that
}

trait Logging {
  @transient var log_ : Logger = null

  def log: Logger = {
    if (log_ == null)
      log_ = LoggerFactory.getLogger("Hail")
    log_
  }
}

class FatalException(msg: String) extends RuntimeException(msg)

class PropagatedTribbleException(msg: String) extends RuntimeException(msg)

object Utils extends Logging {
  implicit def toRichMap[K, V](m: Map[K, V]): RichMap[K, V] = new RichMap(m)

  implicit def toRichMutableMap[K, V](m: mutable.Map[K, V]): RichMutableMap[K, V] = new RichMutableMap(m)

  implicit def toRichRDD[T](r: RDD[T])(implicit tct: ClassTag[T]): RichRDD[T] = new RichRDD(r)

  implicit def toRichRDDByteArray(r: RDD[Array[Byte]]): RichRDDByteArray = new RichRDDByteArray(r)

  implicit def toRichIterable[T](i: Iterable[T]): RichIterable[T] = new RichIterable(i)

  implicit def toRichArrayBuilderOfByte(t: mutable.ArrayBuilder[Byte]): RichArrayBuilderOfByte =
    new RichArrayBuilderOfByte(t)

  implicit def toRichIteratorOfByte(i: Iterator[Byte]): RichIteratorOfByte =
    new RichIteratorOfByte(i)

  implicit def richArray[T](a: Array[T]): RichArray[T] = new RichArray(a)

  implicit def toRichIndexedRow(r: IndexedRow): RichIndexedRow =
    new RichIndexedRow(r)

  implicit def toBDenseVector(v: SDenseVector): BDenseVector[Double] =
    new BDenseVector(v.values)

  implicit def toBSparseVector(v: SSparseVector): BSparseVector[Double] =
    new BSparseVector(v.indices, v.values, v.size)

  implicit def toBVector(v: SVector): BVector[Double] = v match {
    case v: SSparseVector => v
    case v: SDenseVector => v
  }

  implicit def toSDenseVector(v: BDenseVector[Double]): SDenseVector =
    new SDenseVector(v.toArray)

  implicit def toSSparseVector(v: BSparseVector[Double]): SSparseVector =
    new SSparseVector(v.length, v.array.index, v.array.data)

  implicit def toSVector(v: BVector[Double]): SVector = v match {
    case v: BDenseVector[Double] => v
    case v: BSparseVector[Double] => v
  }

  implicit object subBVectorSVector
    extends OpSub.Impl2[BVector[Double], SVector, BVector[Double]] {
    def apply(a: BVector[Double], b: SVector): BVector[Double] = a - toBVector(b)
  }

  implicit object subBVectorIndexedRow
    extends OpSub.Impl2[BVector[Double], IndexedRow, IndexedRow] {
    def apply(a: BVector[Double], b: IndexedRow): IndexedRow =
      new IndexedRow(b.index, a - toBVector(b.vector))
  }

  implicit object addBVectorSVector
    extends OpAdd.Impl2[BVector[Double], SVector, BVector[Double]] {
    def apply(a: BVector[Double], b: SVector): BVector[Double] = a + toBVector(b)
  }

  implicit object addBVectorIndexedRow
    extends OpAdd.Impl2[BVector[Double], IndexedRow, IndexedRow] {
    def apply(a: BVector[Double], b: IndexedRow): IndexedRow =
      new IndexedRow(b.index, a + toBVector(b.vector))
  }

  implicit def toRichEnumeration[T <: Enumeration](e: T): RichEnumeration[T] =
    new RichEnumeration(e)

  implicit def toRichOption[T](o: Option[T]): RichOption[T] =
    new RichOption[T](o)


  implicit def toRichPairTraversableOnce[K, V](t: TraversableOnce[(K, V)]): RichPairTraversableOnce[K, V] =
    new RichPairTraversableOnce[K, V](t)

  implicit def toRichIntPairTraversableOnce[V](t: TraversableOnce[(Int, V)]): RichIntPairTraversableOnce[V] =
    new RichIntPairTraversableOnce[V](t)

  def plural(n: Int, sing: String, plur: String = null): String =
    if (n == 1)
      sing
    else if (plur == null)
      sing + "s"
    else
      plur

  def info(msg: String) {
    log.info(msg)
    System.err.println("hail: info: " + msg)
  }

  def warn(msg: String) {
    log.warn(msg)
    System.err.println("hail: warning: " + msg)
  }

  def error(msg: String) {
    log.error(msg)
    System.err.println("hail: error: " + msg)
  }

  def fatal(msg: String): Nothing = {
    throw new FatalException(msg)
  }

  def fail(): Nothing = {
    assert(false)
    sys.exit(1)
  }

  def hadoopFS(filename: String, hConf: hadoop.conf.Configuration): hadoop.fs.FileSystem =
    new hadoop.fs.Path(filename).getFileSystem(hConf)

  def hadoopCreate(filename: String, hConf: hadoop.conf.Configuration): OutputStream = {
    val fs = hadoopFS(filename, hConf)
    val hPath = new hadoop.fs.Path(filename)
    val os = fs.create(hPath)
    val codecFactory = new CompressionCodecFactory(hConf)
    val codec = codecFactory.getCodec(hPath)

    if (codec != null)
      codec.createOutputStream(os)
    else
      os
  }

  def hadoopOpen(filename: String, hConf: hadoop.conf.Configuration): InputStream = {
    val fs = hadoopFS(filename, hConf)
    val hPath = new hadoop.fs.Path(filename)
    val is = fs.open(hPath)
    val codecFactory = new CompressionCodecFactory(hConf)
    val codec = codecFactory.getCodec(hPath)
    if (codec != null)
      codec.createInputStream(is)
    else
      is
  }

  def hadoopMkdir(dirname: String, hConf: hadoop.conf.Configuration) {
    hadoopFS(dirname, hConf).mkdirs(new hadoop.fs.Path(dirname))
  }

  def hadoopDelete(filename: String, hConf: hadoop.conf.Configuration, recursive: Boolean) {
    hadoopFS(filename, hConf).delete(new hadoop.fs.Path(filename), recursive)
  }

  def hadoopGetTemporaryFile(tmpdir: String, hConf: hadoop.conf.Configuration, nChar: Int = 10,
                             prefix: Option[String] = None, suffix: Option[String] = None): String = {

    val destFS = hadoopFS(tmpdir, hConf)
    val prefixString = if (prefix.isDefined) prefix + "-" else ""
    val suffixString = if (suffix.isDefined) "." + suffix else ""

    def getRandomName: String = {
      val randomName = tmpdir + "/" + prefixString + scala.util.Random.alphanumeric.take(nChar).mkString + suffixString
      val fileExists = destFS.exists(new hadoop.fs.Path(randomName))

      if (!fileExists)
        randomName
      else
        getRandomName
    }
    getRandomName
  }

  def hadoopGlobAndSort(filename: String, hConf: hadoop.conf.Configuration): Array[FileStatus] = {
    val fs = hadoopFS(filename, hConf)
    val path = new hadoop.fs.Path(filename)

    val files = fs.globStatus(path)
    if (files == null)
      return Array.empty[FileStatus]

    files.sortWith(_.compareTo(_) < 0)
  }

  def hadoopCopyMerge(srcFilenames: Array[String], destFilename: String, hConf: hadoop.conf.Configuration, deleteSource: Boolean = true) {

    val destPath = new hadoop.fs.Path(destFilename)
    val destFS = hadoopFS(destFilename, hConf)

    val codecFactory = new CompressionCodecFactory(hConf)
    val codec = Option(codecFactory.getCodec(new hadoop.fs.Path(destFilename)))
    val isBGzip = codec.exists(_.isInstanceOf[BGzipCodec])

    val srcFileStatuses = srcFilenames.flatMap(f => hadoopGlobAndSort(f, hConf))
    require(srcFileStatuses.forall {
      fileStatus => fileStatus.getPath != destPath && fileStatus.isFile
    })

    val outputStream = destFS.create(destPath)

    try {
      var i = 0
      while (i < srcFileStatuses.length) {
        val fileStatus = srcFileStatuses(i)
        val lenAdjust: Long = if (isBGzip && i < srcFileStatuses.length - 1)
          -28
        else
          0
        val srcFS = hadoopFS(fileStatus.getPath.toString, hConf)
        val inputStream = srcFS.open(fileStatus.getPath)
        try {
          copyBytes(inputStream, outputStream,
            fileStatus.getLen + lenAdjust,
            false)
        } finally {
          inputStream.close()
        }
        i += 1
      }
    } finally {
      outputStream.close()
    }

    if (deleteSource) {
      srcFileStatuses.foreach {
        case fileStatus => hadoopDelete(fileStatus.getPath.toString, hConf, true)
      }
    }
  }

  def writeObjectFile[T](filename: String,
                         hConf: hadoop.conf.Configuration)(f: (ObjectOutputStream) => T): T = {
    val oos = new ObjectOutputStream(hadoopCreate(filename, hConf))
    try {
      f(oos)
    } finally {
      oos.close()
    }
  }

  def readObjectFile[T](filename: String,
                        hConf: hadoop.conf.Configuration)(f: (ObjectInputStream) => T): T = {
    val ois = new ObjectInputStream(hadoopOpen(filename, hConf))
    try {
      f(ois)
    } finally {
      ois.close()
    }
  }

  def readDataFile[T](filename: String,
                      hConf: hadoop.conf.Configuration)(f: (spark.serializer.DeserializationStream) => T): T = {
    val serializer = SparkEnv.get.serializer.newInstance()
    val ds = serializer.deserializeStream(hadoopOpen(filename, hConf))
    try {
      f(ds)
    } finally {
      ds.close()
    }
  }

  def writeTextFile[T](filename: String,
                       hConf: hadoop.conf.Configuration)(writer: (OutputStreamWriter) => T): T = {
    val oos = hadoopCreate(filename, hConf)
    val fw = new OutputStreamWriter(oos)
    try {
      writer(fw)
    } finally {
      fw.close()
    }
  }

  def writeDataFile[T](filename: String,
                       hConf: hadoop.conf.Configuration)(f: (spark.serializer.SerializationStream) => T): T = {
    val serializer = SparkEnv.get.serializer.newInstance()
    val ss = serializer.serializeStream(hadoopCreate(filename, hConf))
    try {
      f(ss)
    } finally {
      ss.close()
    }
  }

  def writeFile[T](filename: String,
                   hConf: hadoop.conf.Configuration)(writer: (OutputStream) => T): T = {
    val os = hadoopCreate(filename, hConf)
    try {
      writer(os)
    } finally {
      os.close()
    }
  }

  def readFile[T](filename: String,
                  hConf: hadoop.conf.Configuration)(reader: (InputStream) => T): T = {
    val is = hadoopOpen(filename, hConf)
    try {
      reader(is)
    } finally {
      is.close()
    }
  }

  def writeTable(filename: String, hConf: hadoop.conf.Configuration,
    lines: Traversable[String], header: Option[String] = None) {
    writeTextFile(filename, hConf) {
      fw =>
        header.map { h =>
          fw.write(h)
          fw.write('\n')
        }
        lines.foreach { line =>
          fw.write(line)
          fw.write('\n')
        }
    }
  }

  def square[T](d: T)(implicit ev: T => scala.math.Numeric[T]#Ops): T = d * d

  def simpleAssert(p: Boolean) {
    if (!p) throw new AssertionError
  }

  def printTime[T](block: => T) = {
    val timed = time(block)
    println("time: " + formatTime(timed._2))
    timed._1
  }

  def time[A](f: => A): (A, Long) = {
    val t0 = System.nanoTime()
    val result = f
    val t1 = System.nanoTime()
    (result, t1 - t0)
  }

  final val msPerMinute = 60 * 1e3
  final val msPerHour = 60 * msPerMinute
  final val msPerDay = 24 * msPerHour

  def formatTime(dt: Long): String = {
    val tMilliseconds = dt / 1e6
    if (tMilliseconds < 1000)
      ("%.3f" + "ms").format(tMilliseconds)
    else if (tMilliseconds < msPerMinute)
      ("%.3f" + "s").format(tMilliseconds / 1e3)
    else if (tMilliseconds < msPerHour) {
      val tMins = (tMilliseconds / msPerMinute).toInt
      val tSec = (tMilliseconds % msPerMinute) / 1e3
      ("%d" + "m" + "%.1f" + "s").format(tMins, tSec)
    }
    else {
      val tHrs = (tMilliseconds / msPerHour).toInt
      val tMins = ((tMilliseconds % msPerHour) / msPerMinute).toInt
      val tSec = (tMilliseconds % msPerMinute) / 1e3
      ("%d" + "h" + "%d" + "m" + "%.1f" + "s").format(tHrs, tMins, tSec)
    }
  }

  def space[A](f: => A): (A, Long) = {
    val rt = Runtime.getRuntime
    System.gc()
    System.gc()
    val before = rt.totalMemory() - rt.freeMemory()
    val r = f
    System.gc()
    val after = rt.totalMemory() - rt.freeMemory()
    (r, after - before)
  }

  def printSpace[A](f: => A): A = {
    val (r, ds) = space(f)
    println("space: " + formatSpace(ds))
    r
  }

  def formatSpace(ds: Long) = {
    val absds = ds.abs
    if (absds < 1e3)
      s"${ds}B"
    else if (absds < 1e6)
      s"${ds.toDouble / 1e3}KB"
    else if (absds < 1e9)
      s"${ds.toDouble / 1e6}MB"
    else if (absds < 1e12)
      s"${ds.toDouble / 1e9}GB"
    else
      s"${ds.toDouble / 1e12}TB"
  }

  def someIf[T](p: Boolean, x: => T): Option[T] =
    if (p)
      Some(x)
    else
      None

  def divOption[T](num: T, denom: T)(implicit ev: T => Double): Option[Double] =
    someIf(denom != 0, ev(num) / denom)

  implicit def toRichStringBuilder(sb: mutable.StringBuilder): RichStringBuilder =
    new RichStringBuilder(sb)

  def D_epsilon(a: Double, b: Double, tolerance: Double = 1.0E-6): Double =
    math.max(java.lang.Double.MIN_NORMAL, tolerance * math.max(math.abs(a), math.abs(b)))

  def D_==(a: Double, b: Double, tolerance: Double = 1.0E-6): Boolean =
    math.abs(a - b) <= D_epsilon(a, b, tolerance)

  def D_!=(a: Double, b: Double, tolerance: Double = 1.0E-6): Boolean =
    math.abs(a - b) > D_epsilon(a, b, tolerance)

  def D_<(a: Double, b: Double, tolerance: Double = 1.0E-6): Boolean =
    a - b < -D_epsilon(a, b, tolerance)

  def D_<=(a: Double, b: Double, tolerance: Double = 1.0E-6): Boolean =
    a - b <= D_epsilon(a, b, tolerance)

  def D_>(a: Double, b: Double, tolerance: Double = 1.0E-6): Boolean =
    a - b > D_epsilon(a, b, tolerance)

  def D_>=(a: Double, b: Double, tolerance: Double = 1.0E-6): Boolean =
    a - b >= -D_epsilon(a, b, tolerance)

  def flushDouble(a: Double): Double =
    if (math.abs(a) < java.lang.Double.MIN_NORMAL) 0.0 else a

  def genBase: Gen[Char] = Gen.oneOf('A', 'C', 'T', 'G')

  def genDNAString: Gen[String] = Gen.buildableOf[String, Char](genBase).filter(s => !s.isEmpty)

  implicit def richIterator[T](it: Iterator[T]): RichIterator[T] = new RichIterator[T](it)

  implicit def richBoolean(b: Boolean): RichBoolean = new RichBoolean(b)

  implicit def accumulableMapInt[K]: AccumulableParam[mutable.Map[K, Int], K] = new AccumulableParam[mutable.Map[K, Int], K] {
    def addAccumulator(r: mutable.Map[K, Int], t: K): mutable.Map[K, Int] = {
      r.updateValue(t, 0, _ + 1)
      r
    }

    def addInPlace(r1: mutable.Map[K, Int], r2: mutable.Map[K, Int]): mutable.Map[K, Int] = {
      for ((k, v) <- r2)
        r1.updateValue(k, 0, _ + v)
      r1
    }

    def zero(initialValue: mutable.Map[K, Int]): mutable.Map[K, Int] =
      mutable.Map.empty[K, Int]
  }
}
