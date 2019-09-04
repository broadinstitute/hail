package is.hail.expr.ir

import is.hail.HailContext
import is.hail.linalg.BlockMatrix
import is.hail.utils.richUtils.RichDenseMatrixDouble
import org.json4s.{DefaultFormats, Formats, ShortTypeHints}

object BlockMatrixWriter {
  implicit val formats: Formats = new DefaultFormats() {
    override val typeHints = ShortTypeHints(
      List(classOf[BlockMatrixNativeWriter], classOf[BlockMatrixBinaryWriter], classOf[BlockMatrixRectanglesWriter],
        classOf[BlockMatrixBinaryMultiWriter], classOf[BlockMatrixTextMultiWriter], classOf[BlockMatrixNativeMultiWriter]))
    override val typeHintFieldName: String = "name"
  }
}


abstract class BlockMatrixWriter {
  def apply(hc: HailContext, bm: BlockMatrix): Unit
}

case class BlockMatrixNativeWriter(
  path: String,
  overwrite: Boolean,
  forceRowMajor: Boolean,
  stageLocally: Boolean) extends BlockMatrixWriter {

  def apply(hc: HailContext, bm: BlockMatrix): Unit = bm.write(hc.sFS, path, overwrite, forceRowMajor, stageLocally)
}

case class BlockMatrixBinaryWriter(path: String) extends BlockMatrixWriter {
  def apply(hc: HailContext, bm: BlockMatrix): Unit = {
    RichDenseMatrixDouble.exportToDoubles(hc.sFS, path, bm.toBreezeMatrix(), forceRowMajor = true)
  }
}

case class BlockMatrixRectanglesWriter(
  path: String,
  rectangles: Array[Array[Long]],
  delimiter: String,
  binary: Boolean) extends BlockMatrixWriter {

  def apply(hc: HailContext, bm: BlockMatrix): Unit = {
    bm.exportRectangles(hc, path, rectangles, delimiter, binary)
  }
}

abstract class BlockMatrixMultiWriter {
  def apply(bms: IndexedSeq[BlockMatrix]): Unit
}

case class BlockMatrixBinaryMultiWriter(
  prefix: String,
  overwrite: Boolean) extends BlockMatrixMultiWriter {

  def apply(bms: IndexedSeq[BlockMatrix]): Unit =
    BlockMatrix.binaryWriteBlockMatrices(bms, prefix, overwrite)
}

case class BlockMatrixTextMultiWriter(
  prefix: String,
  overwrite: Boolean,
  delimiter: String,
  header: Option[String],
  addIndex: Boolean,
  compression: Option[String],
  customFilenames: Option[Array[String]]) extends BlockMatrixMultiWriter {

  def apply(bms: IndexedSeq[BlockMatrix]): Unit =
    BlockMatrix.exportBlockMatrices(bms, prefix, overwrite, delimiter, header, addIndex, compression, customFilenames)
}

case class BlockMatrixNativeMultiWriter(
  prefix: String,
  overwrite: Boolean) extends BlockMatrixMultiWriter {

  def apply(bms: IndexedSeq[BlockMatrix]): Unit = {
    BlockMatrix.writeBlockMatrices(bms, prefix, overwrite)
  }
}
