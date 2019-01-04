package is.hail.stats

import breeze.linalg._
import is.hail.annotations.RegionValue
import is.hail.expr._
import is.hail.expr.ir.{MatrixValue, Sym}
import is.hail.expr.types._
import is.hail.expr.types.physical.{PArray, PStruct}
import is.hail.expr.types.virtual.TFloat64
import is.hail.utils._
import is.hail.variant.MatrixTable
import org.apache.spark.sql.Row

object RegressionUtils {  
  def setMeanImputedDoubles(data: Array[Double],
    offset: Int,
    completeColIdx: Array[Int],
    missingCompleteCols: ArrayBuilder[Int],
    rv: RegionValue,
    rvRowType: PStruct,
    entryArrayType: PArray,
    entryType: PStruct,
    entryArrayIdx: Int,
    fieldIdx: Int) : Unit = {

    missingCompleteCols.clear()
    val n = completeColIdx.length
    var sum = 0.0
    val region = rv.region
    val entryArrayOffset = rvRowType.loadField(rv, entryArrayIdx)

    var j = 0
    while (j < n) {
      val k = completeColIdx(j)
      if (entryArrayType.isElementDefined(region, entryArrayOffset, k)) {
        val entryOffset = entryArrayType.loadElement(region, entryArrayOffset, k)
        if (entryType.isFieldDefined(region, entryOffset, fieldIdx)) {
          val fieldOffset = entryType.loadField(region, entryOffset, fieldIdx)
          val e = region.loadDouble(fieldOffset)
          sum += e
          data(offset + j) = e
        } else
          missingCompleteCols += j
      } else
        missingCompleteCols += j
      j += 1
    }

    val nMissing = missingCompleteCols.size
    val mean = sum / (n - nMissing)
    var i = 0
    while (i < nMissing) {
      data(offset + missingCompleteCols(i)) = mean
      i += 1
    }
  }

  // IndexedSeq indexed by column, Array by field
  def getColumnVariables(mv: MatrixValue, names: Array[Sym]): IndexedSeq[Array[Option[Double]]] = {
    val colType = mv.typ.colType
    assert(names.forall(name => mv.typ.colType.field(name).typ.isOfType(TFloat64())))
    val fieldIndices = names.map { name =>
      val field = mv.typ.colType.field(name)
      assert(field.typ.isOfType(TFloat64()))
      field.index
    }
    mv.colValues
      .value
      .map { a =>
        val struct = a.asInstanceOf[Row]
        fieldIndices.map(i => Option(struct.get(i)).map(_.asInstanceOf[Double]))
      }
  }

  def getPhenoCovCompleteSamples(
    vsm: MatrixTable,
    yField: Sym,
    covFields: Array[Sym]): (DenseVector[Double], DenseMatrix[Double], Array[Int]) = {

    val (y, covs, completeSamples) = getPhenosCovCompleteSamples(vsm, Array(yField), covFields)

    (DenseVector(y.data), covs, completeSamples)
  }

  def getPhenosCovCompleteSamples(
    vsm: MatrixTable,
    yFields: Array[Sym],
    covFields: Array[Sym]): (DenseMatrix[Double], DenseMatrix[Double], Array[Int]) = getPhenosCovCompleteSamples(vsm.value, yFields, covFields)

  def getPhenosCovCompleteSamples(
    mv: MatrixValue,
    yFields: Array[Sym],
    covFields: Array[Sym]): (DenseMatrix[Double], DenseMatrix[Double], Array[Int]) = {

    val nPhenos = yFields.length
    val nCovs = covFields.length

    if (nPhenos == 0)
      fatal("No phenotypes present.")
    
    val yIS = getColumnVariables(mv, yFields)
    val covIS = getColumnVariables(mv, covFields)

    val nCols = mv.nCols
    val (yForCompleteSamples, covForCompleteSamples, completeSamples) =
      (yIS, covIS, 0 until nCols)
        .zipped
        .filter((y, c, s) => y.forall(_.isDefined) && c.forall(_.isDefined))

    val n = completeSamples.size
    if (n == 0)
      fatal("No complete samples: each sample is missing its phenotype or some covariate")

    val yArray = yForCompleteSamples.flatMap(_.map(_.get)).toArray
    val y = new DenseMatrix(rows = n, cols = nPhenos, data = yArray, offset = 0, majorStride = nPhenos, isTranspose = true)

    val covArray = covForCompleteSamples.flatMap(_.map(_.get)).toArray
    val cov = new DenseMatrix(rows = n, cols = nCovs, data = covArray, offset = 0, majorStride = nCovs, isTranspose = true)

    if (n < nCols)
      warn(s"${ nCols - n } of $nCols samples have a missing phenotype or covariate.")

    (y, cov, completeSamples.toArray)
  }
}
