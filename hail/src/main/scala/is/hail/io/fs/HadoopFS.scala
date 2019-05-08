package is.hail.io.fs

import is.hail.utils.{fatal, formatTime, getPartNumber, info, time, using, warn}
import java.io._
import java.util.Map
import scala.collection.JavaConverters._

import com.esotericsoftware.kryo.io.{Input, Output}
import is.hail.io.compress.BGzipCodec
import is.hail.utils.{Context, TextInputFilterAndReplace, WithContext}
import net.jpountz.lz4.{LZ4BlockOutputStream, LZ4Compressor}
import org.apache.hadoop
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.io.IOUtils.copyBytes
import org.apache.hadoop.io.compress.CompressionCodecFactory

import scala.io.Source

class HadoopInputStream(is: FSDataInputStream) extends HailInputStream(is) {
  def seek(pos: Long): Unit = {
    is.seek(pos)
  }
}

class HadoopFilePath(path: hadoop.fs.Path) extends FilePath {
  type Configuration = hadoop.conf.Configuration

  override def toString: String = {
    path.toString()
  }

   def getName: String = {
    path.getName()
  }

  def getFileSystem(conf: Configuration): HadoopFileSystem = {
    new HadoopFileSystem(path.toString, conf)
  }
}


class HadoopFileSystem(val filename: String, conf: hadoop.conf.Configuration) extends FileSystem  {
  private val dPath = new hadoop.fs.Path(filename)
  private val hfs = dPath.getFileSystem(conf)

  def open: HailInputStream = {
    val test = hfs.open(dPath)
    new HadoopInputStream(hfs.open(dPath))
  }

  def open(fPath: FilePath): HailInputStream = {
    new HadoopInputStream(hfs.open(new hadoop.fs.Path(fPath.toString())))
  }

  def open(fPath: String): HailInputStream = {
    new HadoopInputStream(hfs.open(new hadoop.fs.Path(fPath)))
  }

  def deleteOnExit(fPath: FilePath): Boolean = {
    hfs.deleteOnExit(new hadoop.fs.Path(fPath.toString()))
  }

  def makeQualified(fPath: FilePath): FilePath = {
    new HadoopFilePath(hfs.makeQualified(new hadoop.fs.Path(fPath.toString())))
  }

  def getPath(path: String): FilePath = {
    new HadoopFilePath(new hadoop.fs.Path(path))
  }
}

class HadoopFileStatus(fs: hadoop.fs.FileStatus) extends FileStatus {
  def getPath: HadoopFilePath = {
    new HadoopFilePath(fs.getPath())
  }

  def getModificationTime: Long = fs.getModificationTime

  def getLen: Long = fs.getLen

  def isDirectory: Boolean = fs.isDirectory

  def isFile: Boolean = fs.isFile

  def getOwner: String = fs.getOwner
}

//class SerializableHadoopFS(@transient var conf: hadoop.conf.Configuration) extends Serializable {
//  private def writeObject(out: ObjectOutputStream) {
//    out.defaultWriteObject()
//    conf.write(out)
//  }
//
//  private def readObject(in: ObjectInputStream) {
//    conf = new hadoop.conf.Configuration(false)
//    conf.readFields(in)
//
//  }
//}


class HadoopFS(@transient var conf: hadoop.conf.Configuration) extends FS {
    private def writeObject(out: ObjectOutputStream) {
      out.defaultWriteObject()
      conf.write(out)
    }

    private def readObject(in: ObjectInputStream) {
      conf = new hadoop.conf.Configuration(false)
      conf.readFields(in)
    }

  private def create(filename: String): OutputStream = {
    val hPath = new hadoop.fs.Path(filename)
    val fs = hPath.getFileSystem(conf)

    val os = fs.create(hPath)
    val codecFactory = new CompressionCodecFactory(conf)
    val codec = codecFactory.getCodec(hPath)

    if (codec != null)
      codec.createOutputStream(os)
    else
      os
  }

  def open(filename: String, checkCodec: Boolean = true): InputStream = {
    val hPath = new hadoop.fs.Path(filename)
    val fs = hPath.getFileSystem(conf)

    val is = try {
      fs.open(hPath)
    } catch {
      case e: FileNotFoundException =>
        if (isDir(filename))
          throw new FileNotFoundException(s"'$filename' is a directory (or native Table/MatrixTable)")
        else
          throw e
    }
    if (checkCodec) {
      val codecFactory = new CompressionCodecFactory(conf)
      val codec = codecFactory.getCodec(hPath)
      if (codec != null)
        codec.createInputStream(is)
      else
        is
    } else
      is
  }

  def getProperty(name: String): String = {
    conf.get(name)
  }

  def setProperty(name: String, value: String): Unit = {
    conf.set(name, value)
  }

  def getProperties: Iterator[Map.Entry[String, String]] = {
    conf.iterator().asScala
  }

  private def _fileSystem(filename: String): hadoop.fs.FileSystem = {
    new hadoop.fs.Path(filename).getFileSystem(conf)
  }

  def fileSystem(filename: String): HadoopFileSystem = {
    new HadoopFileSystem(filename, conf)
  }

  def getFileSize(filename: String): Long =
    fileStatus(filename).getLen

  def listStatus(filename: String): Array[FileStatus] = {
    val hPath = new hadoop.fs.Path(filename)
    val fs = hPath.getFileSystem(conf)
    fs.listStatus(hPath).map( status => new HadoopFileStatus(status) )
  }

  def isDir(filename: String): Boolean = {
    val hPath = new hadoop.fs.Path(filename)
    hPath.getFileSystem(conf).isDirectory(hPath)
  }

  def isFile(filename: String): Boolean = {
    val hPath = new hadoop.fs.Path(filename)
    hPath.getFileSystem(conf).isFile(hPath)
  }

  def exists(files: String*): Boolean = {
    files.forall(filename => {
      _fileSystem(filename).exists(new hadoop.fs.Path(filename))
    })
  }

  /**
    * @return true if a new directory was created, false otherwise
    **/
  def mkDir(dirname: String): Boolean = {
    _fileSystem(dirname).mkdirs(new hadoop.fs.Path(dirname))
  }

  def delete(filename: String, recursive: Boolean) {
    _fileSystem(filename).delete(new hadoop.fs.Path(filename), recursive)
  }

  def getTemporaryFile(tmpdir: String, nChar: Int = 10,
                       prefix: Option[String] = None, suffix: Option[String] = None): String = {

    val destFS = _fileSystem(tmpdir)
    val prefixString = if (prefix.isDefined) prefix.get + "-" else ""
    val suffixString = if (suffix.isDefined) "." + suffix.get else ""

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

  def globAll(filenames: Iterable[String]): Array[String] = {
    filenames.iterator
      .flatMap { arg =>
        val fss = glob(arg)
        val files = fss.map(_.getPath.toString)
        if (files.isEmpty)
          warn(s"'$arg' refers to no files")
        files
      }.toArray
  }

  def globAllStatuses(filenames: Iterable[String]): Array[FileStatus] = {
    filenames.flatMap { filename =>
      val statuses = glob(filename)
      if (statuses.isEmpty)
        warn(s"'$filename' refers to no files")
      statuses
    }.toArray
  }

  def glob(filename: String): Array[FileStatus] = {
    val path = new hadoop.fs.Path(filename)

    val files = path.getFileSystem(conf).globStatus(path)
    if (files == null)
      return Array.empty[FileStatus]

    files.map(fs => new HadoopFileStatus(fs))
  }

  def copy(src: String, dst: String, deleteSource: Boolean = false) {
    hadoop.fs.FileUtil.copy(
      _fileSystem(src), new hadoop.fs.Path(src),
      _fileSystem(dst), new hadoop.fs.Path(dst),
      deleteSource, conf)
  }

  def copyMerge(
                 sourceFolder: String,
                 destinationFile: String,
                 numPartFilesExpected: Int,
                 deleteSource: Boolean = true,
                 header: Boolean = true,
                 partFilesOpt: Option[IndexedSeq[String]] = None
               ) {
    if (!exists(sourceFolder + "/_SUCCESS"))
      fatal("write failed: no success indicator found")

    delete(destinationFile, recursive = true) // overwriting by default

    val headerFileStatus = glob(sourceFolder + "/header")

    if (header && headerFileStatus.isEmpty)
      fatal(s"Missing header file")
    else if (!header && headerFileStatus.nonEmpty)
      fatal(s"Found unexpected header file")

    val partFileStatuses = partFilesOpt match {
      case None => glob(sourceFolder + "/part-*")
      case Some(files) => files.map(f => fileStatus(sourceFolder + "/" + f)).toArray
    }
    val sortedPartFileStatuses = partFileStatuses.sortBy(fs => getPartNumber(fs.getPath.getName)
    )
    if (sortedPartFileStatuses.length != numPartFilesExpected)
      fatal(s"Expected $numPartFilesExpected part files but found ${ sortedPartFileStatuses.length }")

    val filesToMerge = headerFileStatus ++ sortedPartFileStatuses

    val (_, dt) = time {
      copyMergeList(filesToMerge, destinationFile, deleteSource)
    }

    info(s"while writing:\n    $destinationFile\n  merge time: ${ formatTime(dt) }")

    if (deleteSource) {
      delete(sourceFolder, recursive = true)
      if (header)
        delete(sourceFolder + ".header", recursive = false)
    }
  }

  def copyMergeList(srcFileStatuses: Array[FileStatus], destFilename: String, deleteSource: Boolean = true) {
    val destPath = new hadoop.fs.Path(destFilename)
    val destFS = destPath.getFileSystem(conf)

    val codecFactory = new CompressionCodecFactory(conf)
    val codec = Option(codecFactory.getCodec(new hadoop.fs.Path(destFilename)))
    val isBGzip = codec.exists(_.isInstanceOf[BGzipCodec])

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
        val srcFS = fileSystem(fileStatus.getPath.toString)
        val inputStream = srcFS.open(fileStatus.getPath.toString())
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
        fileStatus => delete(fileStatus.getPath.toString, recursive = true)
      }
    }
  }

  def stripCodec(s: String): String = {
    val path = new org.apache.hadoop.fs.Path(s)

    Option(new CompressionCodecFactory(conf)
      .getCodec(path))
      .map { codec =>
        val ext = codec.getDefaultExtension
        assert(s.endsWith(ext))
        s.dropRight(ext.length)
      }.getOrElse(s)
  }

  def getCodec(s: String): String = {
    val path = new org.apache.hadoop.fs.Path(s)

    Option(new CompressionCodecFactory(conf)
      .getCodec(path))
      .map { codec =>
        val ext = codec.getDefaultExtension
        assert(s.endsWith(ext))
        s.takeRight(ext.length)
      }.getOrElse("")
  }

  def fileStatus(filename: String): FileStatus = {
    val p = new hadoop.fs.Path(filename)
    new HadoopFileStatus(p.getFileSystem(conf).getFileStatus(p))
  }

  def readLines[T](filename: String, filtAndReplace: TextInputFilterAndReplace = TextInputFilterAndReplace())(reader: Iterator[WithContext[String]] => T): T = {
    readFile[T](filename) {
      is =>
        val lines = Source.fromInputStream(is)
          .getLines()
          .zipWithIndex
          .map {
            case (value, position) =>
              val source = Context(value, filename, Some(position))
              WithContext(value, source)
          }
        reader(filtAndReplace(lines))
    }
  }

  def writeTable(filename: String, lines: Traversable[String], header: Option[String] = None) {
    writeTextFile(filename) {
      fw =>
        header.foreach { h =>
          fw.write(h)
          fw.write('\n')
        }
        lines.foreach { line =>
          fw.write(line)
          fw.write('\n')
        }
    }
  }

  def writeLZ4DataFile[T](path: String, blockSize: Int, compressor: LZ4Compressor)(writer: (DataOutputStream) => T): T = {
    val oos = create(path)
    val comp = new LZ4BlockOutputStream(oos, blockSize, compressor)
    val dos = new DataOutputStream(comp)
    try {
      writer(dos)
    } finally {
      dos.flush()
      dos.close()
    }
  }

  def writeObjectFile[T](filename: String)(f: (ObjectOutputStream) => T): T =
    using(create(filename)) { ois => using(new ObjectOutputStream(ois))(f) }

  def readObjectFile[T](filename: String)(f: (ObjectInputStream) => T): T =
    using(open(filename)) { is => using(new ObjectInputStream(is))(f) }

  def writeDataFile[T](filename: String)(f: (DataOutputStream) => T): T =
    using(new DataOutputStream(create(filename)))(f)

  def readDataFile[T](filename: String)(f: (DataInputStream) => T): T =
    using(new DataInputStream(open(filename)))(f)

  def writeTextFile[T](filename: String)(f: (OutputStreamWriter) => T): T =
    using(new OutputStreamWriter(create(filename)))(f)

  def readTextFile[T](filename: String)(f: (InputStreamReader) => T): T =
    using(new InputStreamReader(open(filename)))(f)

  def writeKryoFile[T](filename: String)(f: (Output) => T): T =
    using(new Output(create(filename)))(f)

  def readKryoFile[T](filename: String)(f: (Input) => T): T =
    using(new Input(open(filename)))(f)

  def readFile[T](filename: String)(f: (InputStream) => T): T =
    using(open(filename))(f)

  def writeFile[T](filename: String)(f: (OutputStream) => T): T =
    using(create(filename))(f)

  def unsafeReader(filename: String, checkCodec: Boolean = true): InputStream = open(filename, checkCodec)

  def unsafeWriter(filename: String): OutputStream = create(filename)
}