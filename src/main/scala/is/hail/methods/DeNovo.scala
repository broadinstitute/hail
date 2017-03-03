package is.hail.methods

import is.hail.keytable.KeyTable
import is.hail.variant._
import is.hail.expr._
import is.hail.utils._
import is.hail.annotations._
import org.apache.spark.sql.Row

import scala.collection.mutable

object DeNovo {

  def isCompleteGenotype(g: Genotype): Boolean = {
    g.unboxedGT >= 0 && g.unboxedAD != null && g.unboxedDP >= 0 && g.unboxedGQ >= 0 && g.unboxedPL != null
  }

  def schema: TStruct = TStruct(
    "variant" -> TVariant,
    "probandID" -> TString,
    "fatherID" -> TString,
    "motherID" -> TString,
    "isFemale" -> TBoolean,
    "confidence" -> TDouble,
    "probandGt" -> TGenotype,
    "motherGt" -> TGenotype,
    "fatherGt" -> TGenotype,
    "pDeNovo" -> TDouble)

  private val PRIOR = 1.0 / 30000000

  private val MIN_PRIOR = 100.0 / 30000000

  def callAutosomal(kid: Genotype, dad: Genotype, mom: Genotype, isSNP: Boolean, prior: Double,
    minPDeNovo: Double, nAltAlleles: Int, minDpRatio: Double, maxParentAB: Double): Option[(String, Double)] = {

    if (dad == null || mom == null ||
      !(kid.unboxedGT == 1 && dad.unboxedGT == 0 && mom.unboxedGT == 0) ||
      kid.unboxedGT.toDouble / (dad.unboxedGT + mom.unboxedGT) < minDpRatio ||
      (dad.unboxedAD(0) == 0) && (dad.unboxedAD(1) == 0) ||
      (mom.unboxedAD(0) == 0) && (mom.unboxedAD(1) == 0) ||
      dad.unboxedAD(1).toDouble / (dad.unboxedAD(0) + dad.unboxedAD(1)) >= maxParentAB ||
      mom.unboxedAD(1).toDouble / (mom.unboxedAD(0) + mom.unboxedAD(1)) >= maxParentAB)
      return None

    val kidP = kid.unboxedPL.map(x => math.pow(10, -x / 10d))
    val dadP = dad.unboxedPL.map(x => math.pow(10, -x / 10d))
    val momP = mom.unboxedPL.map(x => math.pow(10, -x / 10d))

    val kidSum = kidP.sum
    val dadSum = dadP.sum
    val momSum = momP.sum

    (0 until 3).foreach { i =>
      kidP(i) = kidP(i) / kidSum
      dadP(i) = dadP(i) / dadSum
      momP(i) = momP(i) / momSum
    }

    val pDeNovoData = dadP(0) * momP(0) * kidP(1) * PRIOR

    val pDataOneHet = (dadP(1) * momP(0) + dadP(0) * momP(1)) * kidP(1)
    val pOneParentHet = 1 - math.pow(1 - prior, 4)
    val pMissedHetInParent = pDataOneHet * pOneParentHet

    val pTrueDeNovo = pDeNovoData / (pDeNovoData + pMissedHetInParent)

    val kidAdRatio = kid.unboxedAD(1).toDouble / (kid.unboxedAD(0) + kid.unboxedAD(1))

    val kidDp = kid.unboxedDP
    val dpRatio = kidDp.toDouble / (mom.unboxedDP + dad.unboxedDP)

    // Below is the core calling algorithm
    if (pTrueDeNovo < minPDeNovo)
      None
    else if (dpRatio < minDpRatio)
      None
    else if (!isSNP) {
      if ((pTrueDeNovo > 0.99) && (kidAdRatio > 0.3) && (nAltAlleles == 1))
        Some("HIGH", pTrueDeNovo)
      else if ((pTrueDeNovo > 0.5) && (kidAdRatio > 0.3) && (nAltAlleles <= 5))
        Some("MEDIUM", pTrueDeNovo)
      else if ((pTrueDeNovo > 0.05) && (kidAdRatio > 0.20))
        Some("LOW", pTrueDeNovo)
      else None
    } else {
      if ((pTrueDeNovo > 0.99) && (kidAdRatio > 0.3) && (dpRatio > 0.2) ||
        ((pTrueDeNovo > 0.99) && (kidAdRatio > 0.3) && (nAltAlleles == 1)) ||
        ((pTrueDeNovo > 0.5) && (kidAdRatio >= 0.3) && (nAltAlleles < 10) && (kidDp >= 10))
      )
        Some("HIGH", pTrueDeNovo)
      else if ((pTrueDeNovo > 0.5) && (kidAdRatio > 0.3) ||
        ((pTrueDeNovo > 0.5) && (nAltAlleles == 1))
      )
        Some("MEDIUM", pTrueDeNovo)
      else if ((pTrueDeNovo > 0.05) && (kidAdRatio > 0.20))
        Some("LOW", pTrueDeNovo)
      else None
    }
  }

  def callHemizygous(kid: Genotype, parent: Genotype, isSNP: Boolean, prior: Double,
    minPDeNovo: Double, nAltAlleles: Int, minDpRatio: Double, maxParentAB: Double): Option[(String, Double)] = {

    if (parent == null ||
      !(kid.unboxedGT == 2 && parent.unboxedGT == 0) ||
      kid.unboxedDP.toDouble / parent.unboxedDP < minDpRatio ||
      (parent.unboxedAD(0) == 0) && (parent.unboxedAD(1) == 0) ||
      parent.unboxedAD(1).toDouble / (parent.unboxedAD(0) + parent.unboxedAD(1)) >= maxParentAB)
      return None

    val kidP = kid.unboxedPL.map(x => math.pow(10, -x / 10d))
    val parentP = parent.unboxedPL.map(x => math.pow(10, -x / 10d))

    val kidSum = kidP.sum
    val parentSum = parentP.sum

    (0 until 3).foreach { i =>
      kidP(i) = kidP(i) / kidSum
      parentP(i) = parentP(i) / parentSum
    }

    val pDeNovoData = parentP(0) * kidP(2) * PRIOR

    val pDataOneHet = (parentP(1) + parentP(2)) * kidP(2)
    val pOneParentHet = 1 - math.pow(1 - prior, 4)
    val pMissedHetInParent = pDataOneHet * pOneParentHet

    val pTrueDeNovo = pDeNovoData / (pDeNovoData + pMissedHetInParent)

    val kidAdRatio = kid.unboxedAD(1).toDouble / (kid.unboxedAD(0) + kid.unboxedAD(1))

    val kidDp = kid.unboxedDP
    val dpRatio = kidDp.toDouble / parent.unboxedDP

    // Below is the core calling algorithm
    if (pTrueDeNovo < minPDeNovo || kidAdRatio <= 0.95)
      None
    else if (!isSNP) {
      if ((pTrueDeNovo > 0.99) && (kidAdRatio > 0.3) && (nAltAlleles == 1))
        Some("HIGH", pTrueDeNovo)
      else if ((pTrueDeNovo > 0.5) && (kidAdRatio > 0.3) && (nAltAlleles <= 5))
        Some("MEDIUM", pTrueDeNovo)
      else if ((pTrueDeNovo > 0.05) && (kidAdRatio > 0.20))
        Some("LOW", pTrueDeNovo)
      else None
    } else {
      if ((pTrueDeNovo > 0.99) && (kidAdRatio > 0.3) && (dpRatio > 0.2) ||
        ((pTrueDeNovo > 0.99) && (kidAdRatio > 0.3) && (nAltAlleles == 1)) ||
        ((pTrueDeNovo > 0.5) && (kidAdRatio >= 0.3) && (nAltAlleles < 10) && (kidDp >= 10)))
        Some("HIGH", pTrueDeNovo)
      else if ((pTrueDeNovo > 0.5) && (kidAdRatio > 0.3) ||
        ((pTrueDeNovo > 0.5) && (kidAdRatio > minDpRatio) && (nAltAlleles == 1)))
        Some("MEDIUM", pTrueDeNovo)
      else if ((pTrueDeNovo > 0.05) && (kidAdRatio > 0.20))
        Some("LOW", pTrueDeNovo)
      else None
    }
  }


  def apply(vds: VariantDataset, ped: Pedigree,
    referenceAFExpr: String,
    minGQ: Int = 20,
    minPDeNovo: Double = 0.05,
    maxParentAB: Double = 0.05,
    minChildAB: Double = 0.20,
    minDepthRatio: Double = 0.10): KeyTable = {
    require(vds.wasSplit)

    val (popFrequencyT, popFrequencyF) = vds.queryVA(referenceAFExpr)
    if (popFrequencyT != TDouble)
      fatal(s"population frequency should be a Double, but got `$popFrequencyT'")

    // it's okay to cast null to 0.0 here because missing is treated as 0.0
    val popFreqQuery: (Annotation) => Double = popFrequencyF(_).asInstanceOf[Double]

    val trios = ped.filterTo(vds.sampleIds.toSet).completeTrios
    val nSamplesDiscarded = ped.trios.length - trios.length
    val nTrios = trios.size

    info(s"Calling de novo events for $nTrios complete trios")

    val sampleTrioRoles = mutable.Map.empty[String, List[(Int, Int)]]

    // need a map from Sample position(int) to (int, int)
    trios.zipWithIndex.foreach { case (t, ti) =>
      sampleTrioRoles += (t.kid -> ((ti, 0) :: sampleTrioRoles.getOrElse(t.kid, List.empty[(Int, Int)])))
      sampleTrioRoles += (t.knownDad -> ((ti, 1) :: sampleTrioRoles.getOrElse(t.knownDad, List.empty[(Int, Int)])))
      sampleTrioRoles += (t.knownMom -> ((ti, 2) :: sampleTrioRoles.getOrElse(t.knownMom, List.empty[(Int, Int)])))
    }

    val idMapping = vds.sampleIds.zipWithIndex.toMap

    val sc = vds.sparkContext
    val trioIndexBc = sc.broadcast(trios.map(t => (idMapping(t.kid), idMapping(t.knownDad), idMapping(t.knownMom))))
    val sampleTrioRolesBc = sc.broadcast(vds.sampleIds.map(sampleTrioRoles.getOrElse(_, Nil)).toArray)
    val triosBc = sc.broadcast(trios)
    val trioSexBc = sc.broadcast(trios.map(_.sex.orNull).toArray)

    val rdd = vds.rdd.mapPartitions { iter =>
      val arr = MultiArray2.fill[Genotype](trios.length, 3)(null)

      iter.flatMap { case (v, (va, gs)) =>

        var totalAlleles = 0
        var nAltAlleles = 0

        var ii = 0
        while (ii < nTrios) {
          var jj = 0
          while (jj < 3) {
            arr.update(ii, jj, null)
            jj += 1
          }
          ii += 1
        }

        var i = 0
        gs.foreach { g =>
          if (DeNovo.isCompleteGenotype(g)) {

            val roles = sampleTrioRolesBc.value(i)
            roles.foreach { case (ri, ci) => arr.update(ri, ci, g) }

            nAltAlleles += g.unboxedGT
            totalAlleles += 2
          }
          i += 1
        }

        // correct for the observed genotype
        val computedFrequency = (nAltAlleles.toDouble - 1) / totalAlleles.toDouble

        val popFrequency = popFreqQuery(va)
        if (popFrequency < 0 || popFrequency > 1)
          fatal(
            s"""invalid population frequency value `$popFrequency' for variant $v
                  Population prior must fall between 0 and 1.""".stripMargin)

        val frequency = math.max(math.max(computedFrequency, popFrequency), MIN_PRIOR)

        (0 until nTrios).flatMap { t =>
          val kid = arr(t, 0)
          val dad = arr(t, 1)
          val mom = arr(t, 2)

          val isSNP = v.altAllele.isSNP
          val annotation =
            if (kid == null || kid.unboxedGT == 0 || kid.unboxedGQ <= minGQ ||
              (kid.unboxedAD(0) == 0 && kid.unboxedAD(1) == 0) ||
              kid.unboxedAD(1).toDouble / (kid.unboxedAD(0) + kid.unboxedAD(1)) <= minChildAB)
              None
            else if (v.isAutosomal || v.inXPar)
              callAutosomal(kid, dad, mom, isSNP, frequency,
                minPDeNovo, nAltAlleles, minDepthRatio, maxParentAB)
            else {
              val sex = trioSexBc.value(t)
              if (v.inXNonPar) {
                if (sex == Sex.Female)
                  callAutosomal(kid, dad, mom, isSNP, frequency,
                    minPDeNovo, nAltAlleles, minDepthRatio, maxParentAB)
                else if (sex == Sex.Male)
                  callHemizygous(kid, mom, isSNP, frequency, minPDeNovo, nAltAlleles, minDepthRatio, maxParentAB)
                else None
              } else if (v.inYNonPar) {
                if (sex == Sex.Female)
                  None
                else if (sex == Sex.Male)
                  callHemizygous(kid, dad, isSNP, frequency, minPDeNovo, nAltAlleles, minDepthRatio, maxParentAB)
                else None
              } else if (v.isMitochondrial)
                callHemizygous(kid, mom, isSNP, frequency, minPDeNovo, nAltAlleles, minDepthRatio, maxParentAB)
              else None
            }

          annotation.map { case (confidence, p) =>
            Row(
              v,
              triosBc.value(t).kid,
              triosBc.value(t).dad,
              triosBc.value(t).mom,
              triosBc.value(t).sex.map(s => (s == Sex.Female): Annotation).orNull,
              confidence,
              kid,
              dad,
              mom,
              p)
          }
        }.iterator
      }
    }.cache()

    KeyTable(vds.hc, rdd, schema, Array.empty)
  }
}
