package is.hail.expr.ir.functions

import is.hail.HailContext
import is.hail.expr.ir.MatrixValue
import is.hail.expr.types.MatrixType
import is.hail.expr.types.virtual.{TVoid, Type}
import is.hail.linalg.{BlockMatrix, BlockMatrixMetadata, GridPartitioner, WriteBlocksRDD}
import is.hail.utils._
import org.json4s.jackson

class MatrixWriteBlockMatrix(path: String,
  overwrite: Boolean,
  entryField: String,
  blockSize: Int) extends MatrixToValueFunction {
  def typ(childType: MatrixType): Type = TVoid

  def execute(mv: MatrixValue): Any = {
    val rvd = mv.rvd

    // FIXME
    val partitionCounts = rvd.countPerPartition()

    val hc = HailContext.get
    val sc = hc.sc
    val hadoopConf = hc.hadoopConf

    val partStarts = partitionCounts.scanLeft(0L)(_ + _)
    assert(partStarts.length == rvd.getNumPartitions + 1)

    val nRows = partStarts.last
    val localNCols = mv.nCols

    if (overwrite)
      hadoopConf.delete(path, recursive = true)
    else if (hadoopConf.exists(path))
      fatal(s"file already exists: $path")

    hadoopConf.mkDir(path)

    // write blocks
    hadoopConf.mkDir(path + "/parts")
    val gp = GridPartitioner(blockSize, nRows, localNCols)
    val blockPartFiles =
      new WriteBlocksRDD(path, rvd, sc, partStarts, entryField, gp)
        .collect()

    val blockCount = blockPartFiles.length
    val partFiles = new Array[String](blockCount)
    blockPartFiles.foreach { case (i, f) => partFiles(i) = f }

    // write metadata
    hadoopConf.writeDataFile(path + BlockMatrix.metadataRelativePath) { os =>
      implicit val formats = defaultJSONFormats
      jackson.Serialization.write(
        BlockMatrixMetadata(blockSize, nRows, localNCols, gp.maybeBlocks, partFiles),
        os)
    }

    assert(blockCount == gp.numPartitions)
    info(s"Wrote all $blockCount blocks of $nRows x $localNCols matrix with block size $blockSize.")

    hadoopConf.writeTextFile(path + "/_SUCCESS")(out => ())

    null
  }
}
