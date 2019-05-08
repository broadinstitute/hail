package is.hail.io.compress

import is.hail.SparkSuite
import is.hail.check.Gen
import is.hail.check.Prop.forAll
import is.hail.utils._
import is.hail.io.fs.HadoopFilePath
import htsjdk.samtools.util.BlockCompressedFilePointerUtil
import org.apache.commons.io.IOUtils
import org.apache.{hadoop => hd}
import org.testng.annotations.Test

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.io.Source

class TestFileInputFormat extends hd.mapreduce.lib.input.TextInputFormat {
  override def getSplits(job: hd.mapreduce.JobContext): java.util.List[hd.mapreduce.InputSplit] = {
    val hConf = job.getConfiguration

    val splitPoints = hConf.get("bgz.test.splits").split(",").map(_.toLong)
    println(splitPoints.toSeq)

    val splits = new mutable.ArrayBuffer[hd.mapreduce.InputSplit]
    val files = listStatus(job).asScala
    assert(files.length == 1)

    val file = files.head
    val path = file.getPath
    val length = file.getLen
    val fs = path.getFileSystem(hConf)
    val blkLocations = fs.getFileBlockLocations(file, 0, length)

    Array.tabulate(splitPoints.length - 1) { i =>
      val s = splitPoints(i)
      val e = splitPoints(i + 1)
      val splitSize = e - s

      val blkIndex = getBlockIndex(blkLocations, s)
      splits += makeSplit(path, s, splitSize, blkLocations(blkIndex).getHosts, blkLocations(blkIndex).getCachedHosts)
    }

    splits.asJava
  }
}

class BGzipCodecSuite extends SparkSuite {
  @Test def test() {
    sc.hadoopConfiguration.setLong("mapreduce.input.fileinputformat.split.minsize", 1L)

    val uncompPath = "src/test/resources/sample.vcf"

    /*
     * bgz.test.sample.vcf.bgz was created as follows:
     *  - split sample.vcf into 60-line chunks: `split -l 60 sample.vcf sample.vcf.`
     *  - move the line boundary on two chunks by 1 character in different directions
     *  - bgzip compressed the chunks
     *  - stripped the empty terminate block in chunks except ad and ag (the last)
     *  - concatenated the chunks
     */
    val compPath = "src/test/resources/bgz.test.sample.vcf.bgz"

    val uncompHPath = new HadoopFilePath(new hd.fs.Path(uncompPath))
    val compHPath = new HadoopFilePath(new hd.fs.Path(compPath))

    val fs = uncompHPath.getFileSystem(hc.sc.hadoopConfiguration)

    val uncompIS = fs.open(uncompHPath)
    val uncomp = IOUtils.toByteArray(uncompIS)
    uncompIS.close()

    val decompIS = new BGzipInputStream(fs.open(compHPath))
    val decomp = IOUtils.toByteArray(decompIS)
    decompIS.close()

    assert(uncomp.sameElements(decomp))

    val lines = Source.fromBytes(uncomp).getLines.toArray

    assert(sc.textFile(uncompPath).collectOrdered()
      .sameElements(lines))

    for (i <- 1 until 20) {
      val linesRDD = sc.textFile(compPath, i)
      assert(linesRDD.partitions.length == i)
      assert(linesRDD.collectOrdered().sameElements(lines))
    }

    val compLength = 195353
    val compSplits = Array[Long](6566, 20290, 33438, 41165, 56691, 70278, 77419, 92522, 106310, 112477, 112505, 124593,
      136405, 144293, 157375, 169172, 175174, 186973, 195325)

    val g = for (n <- Gen.oneOfGen(
      Gen.choose(0, 10),
      Gen.choose(0, 100));
      rawSplits <- Gen.buildableOfN[Array](n,
        Gen.oneOfGen(Gen.choose(0L, compLength),
          Gen.applyGen(Gen.oneOf[(Long) => Long](identity, _ - 1, _ + 1),
            Gen.oneOfSeq(compSplits)))))
      yield
        (Array(0L, compLength) ++ rawSplits).distinct.sorted

    val p = forAll(g) { splits =>

      val jobConf = new hd.conf.Configuration(hc.sc.hadoopConfiguration)
      jobConf.set("bgz.test.splits", splits.mkString(","))
      val rdd = sc.newAPIHadoopFile[hd.io.LongWritable, hd.io.Text, TestFileInputFormat](
        compPath,
        classOf[TestFileInputFormat],
        classOf[hd.io.LongWritable],
        classOf[hd.io.Text],
        jobConf)

      val rddLines = rdd.map(_._2.toString).collectOrdered()
      rddLines.sameElements(lines)
    }
    p.check()
  }

  @Test def testVirtualSeek() {
    // real offsets of the start of some blocks, paired with the offset to the next block
    val blockStarts = Array[(Long, Long)]((0, 14653), (69140, 82949), (133703, 146664), (181362, 192983 /* end of file */))
    // NOTE: maxBlockSize is the length of all blocks other than the last
    val maxBlockSize = 65280
    // number determined by counting bytes from sample.vcf from uncompBlockStarts.last to the end of the file
    val lastBlockLen = 55936
    // offsets into the uncompressed file
    val uncompBlockStarts = Array[Int](0, 326400, 652800, 913920)

    val uncompPath = "src/test/resources/sample.vcf"
    val compPath = "src/test/resources/sample.vcf.gz"

    val uncompHPath = new HadoopFilePath(new hd.fs.Path(uncompPath))
    val compHPath = new HadoopFilePath(new hd.fs.Path(compPath))

    val fs = uncompHPath.getFileSystem(hc.sc.hadoopConfiguration)
    val compIS = compHPath.getFileSystem(hc.sc.hadoopConfiguration).open

    using (fs.open(uncompHPath)) { uncompIS => using (new BGzipInputStream(compIS)) { decompIS =>
      val fromEnd = 48 // arbitrary number of bytes from the end of block to attempt to seek to
      for (((cOff, nOff), uOff) <- blockStarts.zip(uncompBlockStarts);
           e <- Seq(0, 1024, maxBlockSize - fromEnd)) {
        val decompData = new Array[Byte](100)
        val uncompData = new Array[Byte](100)
        val extra = if (cOff == blockStarts.last._1 && e == maxBlockSize - fromEnd)
            lastBlockLen - fromEnd
          else
            e
        val vOff = BlockCompressedFilePointerUtil.makeFilePointer(cOff, extra)

        decompIS.virtualSeek(vOff)
        assert(decompIS.getVirtualOffset() == vOff);
        uncompIS.seek(uOff + extra)

        val decompRead = decompIS.readRepeatedly(decompData)
        val uncompRead = uncompIS.readRepeatedly(uncompData)

        assert(decompRead == uncompRead, s"""compressed offset: ${ cOff }
          |decomp bytes read: ${ decompRead }
          |uncomp bytes read: ${ uncompRead }\n""".stripMargin)
        assert(decompData sameElements uncompData, s"data differs for compressed offset: ${ cOff }")
        val expectedVirtualOffset = if (extra == lastBlockLen - fromEnd)
            BlockCompressedFilePointerUtil.makeFilePointer(nOff, 0)
          else if (extra == maxBlockSize - fromEnd)
            BlockCompressedFilePointerUtil.makeFilePointer(nOff, decompRead - fromEnd)
          else
            BlockCompressedFilePointerUtil.makeFilePointer(cOff, extra + decompRead)
        assert(expectedVirtualOffset == decompIS.getVirtualOffset())
      }

      // here we test reading from the middle of a block to it's end
      val decompData = new Array[Byte](maxBlockSize)
      val toSkip = 20000
      val vOff = BlockCompressedFilePointerUtil.makeFilePointer(blockStarts(2)._1, toSkip)
      decompIS.virtualSeek(vOff)
      assert(decompIS.getVirtualOffset() == vOff)
      assert(decompIS.read(decompData) == maxBlockSize - 20000)
      assert(decompIS.getVirtualOffset() == BlockCompressedFilePointerUtil.makeFilePointer(blockStarts(2)._2, 0))

      // Trying to seek to the end of a block should fail
      intercept[java.io.IOException] {
        val vOff = BlockCompressedFilePointerUtil.makeFilePointer(blockStarts(1)._1, maxBlockSize)
        decompIS.virtualSeek(vOff)
      }

      // Trying to seek past the end of a block should fail
      intercept[java.io.IOException] {
        val vOff = BlockCompressedFilePointerUtil.makeFilePointer(blockStarts(0)._1, maxBlockSize + 1)
        decompIS.virtualSeek(vOff)
      }

      // Trying to seek to the end of the last block should fail
      intercept[java.io.IOException] {
        val vOff = BlockCompressedFilePointerUtil.makeFilePointer(blockStarts.last._1, lastBlockLen)
        decompIS.virtualSeek(vOff)
      }

      // trying to seek to the end of file should succeed
      decompIS.virtualSeek(0)
      val eofOffset = BlockCompressedFilePointerUtil.makeFilePointer(blockStarts.last._2, 0)
      decompIS.virtualSeek(eofOffset)
      assert(-1 == decompIS.read())

      // seeking past end of file directly should fail
      decompIS.virtualSeek(0)
      intercept[java.io.IOException] {
        val vOff = BlockCompressedFilePointerUtil.makeFilePointer(blockStarts.last._2, 1)
        decompIS.virtualSeek(vOff)
      }
    }}
  }
}
