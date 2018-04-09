package is.hail.methods

import is.hail.annotations._
import is.hail.expr.types._
import is.hail.expr.{EvalContext, Parser}
import is.hail.rvd.{OrderedRVD, RVD}
import is.hail.utils._
import is.hail.variant._
import org.apache.spark.sql.Row

object FilterAlleles {
  def apply(vsm: MatrixTable, filterExpr: String,
    variantExpr: String = "",
    genotypeExpr: String = "",
    keep: Boolean = true, leftAligned: Boolean = false, keepStar: Boolean = false): MatrixTable = {

    val Array(locusType, allelesType) = vsm.rowKeyTypes

    val conditionEC = EvalContext(Map(
      "global" -> (0, vsm.globalType),
      "va" -> (1, vsm.rowType),
      "aIndex" -> (2, TInt32())))
    val conditionE = Parser.parseTypedExpr[java.lang.Boolean](filterExpr, conditionEC)

    val vEC = EvalContext(Map(
      "global" -> (0, vsm.globalType),
      "va" -> (1, vsm.rowType),
      "newLocus" -> (2, locusType),
      "newAlleles" -> (3, allelesType),
      "oldToNew" -> (4, TArray(TInt32())),
      "newToOld" -> (5, TArray(TInt32()))))

    val gEC = EvalContext(Map(
      "global" -> (0, vsm.globalType),
      "va" -> (1, vsm.rowType),
      "newLocus" -> (2, locusType),
      "newAlleles" -> (3, allelesType),
      "oldToNew" -> (4, TArray(TInt32())),
      "newToOld" -> (5, TArray(TInt32())),
      "sa" -> (6, vsm.colType),
      "g" -> (7, vsm.entryType)))

    val vAnnotator = new ExprAnnotator(vEC, vsm.rowType, variantExpr, Some(Annotation.ROW_HEAD))
    val gAnnotator = new ExprAnnotator(gEC, vsm.entryType, genotypeExpr, Some(Annotation.ENTRY_HEAD))

    val (t1, insertLocus) = vAnnotator.newT.insert(locusType, "locus")
    assert(t1 == vAnnotator.newT, s"\n$t1\n${vAnnotator.newT}")
    val (t2, insertAlleles) = vAnnotator.newT.insert(allelesType, "alleles")
    assert(t2 == vAnnotator.newT)

    val globalsBc = vsm.globals.broadcast
    val localNSamples = vsm.numCols

    val newEntryType = gAnnotator.newT
    val newMatrixType = vsm.matrixType.copyParts(rowType = vAnnotator.newT, entryType = newEntryType)

    def filter(rdd: RVD,
      removeLeftAligned: Boolean, removeMoving: Boolean, verifyLeftAligned: Boolean): RVD = {

      def filterAllelesInVariant(locus: Locus, alleles: IndexedSeq[String], va: Annotation): Option[(Locus, IndexedSeq[String], IndexedSeq[Int], IndexedSeq[Int])] = {
        var alive = 0
        val oldToNew = new Array[Int](alleles.length)
        for (aai <- alleles.tail.indices) {
          val index = aai + 1
          conditionEC.setAll(globalsBc.value, va, index)
          oldToNew(index) =
            if (Filter.boxedKeepThis(conditionE(), keep)) {
              alive += 1
              alive
            } else
              0
        }

        if (alive == 0)
          return None

        val newToOld = oldToNew.iterator
          .zipWithIndex
          .filter { case (newIdx, oldIdx) => oldIdx == 0 || newIdx != 0 }
          .map(_._2)
          .toArray

        val ref = alleles(0)
        val altAlleles = oldToNew.iterator
          .zipWithIndex
          .filter { case (newIdx, _) => newIdx != 0 }
          .map { case (_, idx) => alleles(idx) }
          .toArray

        if (altAlleles.forall(a => AltAlleleMethods.isStar(ref, a)) && !keepStar)
          return None

        val (filtLocus, filtAlleles) = VariantMethods.minRep(locus, ref +: altAlleles)
        Some((filtLocus, filtAlleles, newToOld, oldToNew))
      }

      val fullRowType = vsm.matrixType.rvRowType
      val newRVType = newMatrixType.rvRowType
      val localEntriesIndex = vsm.entriesIndex

      val localSampleAnnotationsBc = vsm.colValues.broadcast

      rdd.mapPartitions(newRVType) { it =>
        var prevLocus: Locus = null
        val fullRow = new UnsafeRow(fullRowType)
        val rvv = new RegionValueVariant(fullRowType)

        it.flatMap { rv =>
          val rvb = new RegionValueBuilder()
          val rv2 = RegionValue()

          rvv.setRegion(rv)
          fullRow.set(rv)

          val gs = fullRow.getAs[IndexedSeq[Annotation]](localEntriesIndex)

          filterAllelesInVariant(rvv.locus(), rvv.alleles(), fullRow)
            .flatMap { case (newLocus, newAlleles, newToOld, oldToNew) =>
              val isLeftAligned = (prevLocus == null || prevLocus != newLocus) &&
                newLocus == rvv.locus

              prevLocus = newLocus

              if (verifyLeftAligned && !isLeftAligned)
                fatal(s"found non-left aligned variant: ${ rvv.locus() }:${ rvv.alleles()(0) }:${ rvv.alleles().tail.mkString(",") } ")

              if ((isLeftAligned && removeLeftAligned)
                || (!isLeftAligned && removeMoving))
                None
              else {
                rvb.set(rv.region)
                rvb.start(newRVType)
                rvb.startStruct()

                vAnnotator.ec.setAll(globalsBc.value, fullRow, newLocus, newAlleles, oldToNew, newToOld)
                var newVA = vAnnotator.insert(fullRow)
                newVA = insertLocus(newVA, newLocus)
                newVA = insertAlleles(newVA, newAlleles)
                val newRow = newVA.asInstanceOf[Row]
                assert(newRow.length == newRVType.size - 1)

                var i = 0
                while (i < newRVType.size - 1) {
                  rvb.addAnnotation(newRVType.types(i), newRow.get(i))
                  i += 1
                }

                gAnnotator.ec.setAll(globalsBc.value, fullRow, newLocus, newAlleles, oldToNew, newToOld)

                rvb.startArray(localNSamples) // gs
                var k = 0
                while (k < localNSamples) {
                  val g = gs(k)
                  gAnnotator.ec.set(6, localSampleAnnotationsBc.value(k))
                  gAnnotator.ec.set(7, g)
                  rvb.addAnnotation(newEntryType, gAnnotator.insert(g))
                  k += 1
                }
                rvb.endArray()
                rvb.endStruct()
                rv2.set(rv.region, rvb.end())

                Some(rv2)
              }
            }
        }
      }
    }

    val newRDD2: OrderedRVD =
      if (leftAligned) {
        OrderedRVD(newMatrixType.orvdType,
          vsm.rvd.partitioner,
          filter(vsm.rvd, removeLeftAligned = false, removeMoving = false, verifyLeftAligned = true))
      } else {
        val leftAlignedVariants = OrderedRVD(newMatrixType.orvdType,
          vsm.rvd.partitioner,
          filter(vsm.rvd, removeLeftAligned = false, removeMoving = true, verifyLeftAligned = false))

        val movingVariants = OrderedRVD.shuffle(newMatrixType.orvdType,
          vsm.rvd.partitioner,
          filter(vsm.rvd, removeLeftAligned = true, removeMoving = false, verifyLeftAligned = false))

        leftAlignedVariants.partitionSortedUnion(movingVariants)
      }

    vsm.copyMT(rvd = newRDD2, matrixType = newMatrixType)
  }
}
