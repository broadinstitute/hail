package is.hail.linalg

import is.hail.annotations.Region
import is.hail.asm4s.{Code, MethodBuilder}
import is.hail.asm4s._
import is.hail.expr.ir.{EmitCodeBuilder, EmitMethodBuilder}
import is.hail.types.physical.{PBaseStructValue, PNDArrayValue, PType}

object LinalgCodeUtils {
  def checkColumnMajor(pndv: PNDArrayValue, cb: EmitCodeBuilder): Value[Boolean] = {
    val answer = cb.newField[Boolean]("checkColumnMajorResult")
    val shapes = pndv.shapes()
    val runningProduct = cb.newLocal[Long]("check_column_major_running_product")

    val elementType = pndv.pt.elementType
    val nDims = pndv.pt.nDims
    cb.append(Code(
      runningProduct := elementType.byteSize,
      Code.foreach(0 until nDims){ index =>
        Code(
          answer := answer & (shapes(index) ceq runningProduct),
          runningProduct := runningProduct * (shapes(index) > 0L).mux(shapes(index), 1L)
        )
      }
    ))
    answer
  }

  def copyRowMajorToColumnMajor(rowMajorFirstElementAddress: Code[Long], targetFirstElementAddress: Code[Long], nRows: Code[Long], nCols: Code[Long], elementPType: PType, mb: MethodBuilder[_]): Code[Unit] = {
    val rowIndex = mb.genFieldThisRef[Long]()
    val colIndex = mb.genFieldThisRef[Long]()
    val rowMajorCoord: Code[Long] = nCols * rowIndex + colIndex
    val colMajorCoord: Code[Long] = nRows * colIndex + rowIndex
    val currentElement = Region.loadPrimitive(elementPType)(rowMajorFirstElementAddress + rowMajorCoord * elementPType.byteSize)

    Code.forLoop(rowIndex := 0L, rowIndex < nRows, rowIndex := rowIndex + 1L,
      Code.forLoop(colIndex := 0L, colIndex < nCols, colIndex := colIndex + 1L,
        Region.storePrimitive(elementPType, (targetFirstElementAddress + colMajorCoord * elementPType.byteSize))(currentElement)
      )
    )
  }

  def copyRowMajorToColumnMajor(rowMajorFirstElementAddress: Long, targetFirstElementAddress: Long, nRows: Long, nCols: Long, elementByteSize: Long): Unit = {

    var rowIndex = 0L
    while(rowIndex < nRows) {
      var colIndex = 0L
      while(colIndex < nCols) {
        val rowMajorCoord = nCols * rowIndex + colIndex
        val colMajorCoord = nRows * colIndex + rowIndex
        val sourceAddress = rowMajorFirstElementAddress + rowMajorCoord * elementByteSize
        val targetAddress = targetFirstElementAddress + colMajorCoord * elementByteSize
        Region.copyFrom(sourceAddress, targetAddress, elementByteSize)
        colIndex += 1
      }
      rowIndex += 1
    }
  }

  def copyColumnMajorToRowMajor(colMajorFirstElementAddress: Code[Long], targetFirstElementAddress: Code[Long], nRows: Code[Long], nCols: Code[Long], elementPType: PType, mb: MethodBuilder[_]): Code[Unit] = {
    val rowIndex = mb.genFieldThisRef[Long]()
    val colIndex = mb.genFieldThisRef[Long]()
    val rowMajorCoord: Code[Long] = nCols * rowIndex + colIndex
    val colMajorCoord: Code[Long] = nRows * colIndex + rowIndex
    val currentElement = Region.loadPrimitive(elementPType)(colMajorFirstElementAddress + colMajorCoord * elementPType.byteSize)

    Code.forLoop(rowIndex := 0L, rowIndex < nRows, rowIndex := rowIndex + 1L,
      Code.forLoop(colIndex := 0L, colIndex < nCols, colIndex := colIndex + 1L,
        Region.storePrimitive(elementPType, targetFirstElementAddress + rowMajorCoord * elementPType.byteSize)(currentElement)
      )
    )
  }

  def copyColumnMajorToRowMajor(colMajorFirstElementAddress: Long, targetFirstElementAddress: Long, nRows: Long, nCols: Long, elementByteSize: Long): Unit = {
    var rowIndex = 0L
    while (rowIndex < nRows) {
      var colIndex = 0L
      while (colIndex < nCols) {
        val rowMajorCoord = nCols * rowIndex + colIndex
        val colMajorCoord = nRows * colIndex + rowIndex
        val sourceAddress = colMajorFirstElementAddress + colMajorCoord * elementByteSize
        val targetAddress = targetFirstElementAddress + rowMajorCoord * elementByteSize
        Region.copyFrom(sourceAddress, targetAddress, elementByteSize)
        colIndex += 1
      }
      rowIndex += 1
    }
  }
}
