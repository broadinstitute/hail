package is.hail.variant

import is.hail.annotations.{Annotation, _}
import is.hail.expr.{EvalContext, Parser, TAggregable, TString, TStruct, Type, _}
import is.hail.table.Table
import is.hail.methods._
import is.hail.rvd.OrderedRVD
import is.hail.stats.ComputeRRM
import is.hail.utils._
import org.apache.spark.sql.Row

import scala.collection.JavaConverters._
import scala.language.implicitConversions
import scala.language.existentials

object VariantDataset {

  def fromKeyTable(kt: Table): MatrixTable = {
    val vType: Type = kt.keyFields.map(_.typ) match {
      case Array(t@TVariant(_, _)) => t
      case arr => fatal("Require one key column of type Variant to produce a variant dataset, " +
        s"but found [ ${ arr.mkString(", ") } ]")
    }

    val rdd = kt.keyedRDD()
      .map { case (k, v) => (k.asInstanceOf[Row].get(0), v) }
      .filter(_._1 != null)
      .mapValues(a => (a: Annotation, Iterable.empty[Annotation]))

    val metadata = VSMMetadata(
      saSignature = TStruct.empty(),
      vSignature = vType,
      vaSignature = kt.valueSignature,
      globalSignature = TStruct.empty())

    MatrixTable.fromLegacy(kt.hc, metadata,
      VSMLocalValue(Annotation.empty, Array.empty[Annotation], Array.empty[Annotation]), rdd)
  }
}

class VariantDatasetFunctions(private val vsm: MatrixTable) extends AnyVal {

  def concordance(other: MatrixTable): (IndexedSeq[IndexedSeq[Long]], Table, Table) = {
    CalculateConcordance(vsm, other)
  }

<<<<<<< 27fdb031ad9f345ab49757bbd6af736bc2d4b446
=======
  def exportPlink(path: String, famExpr: String = "id = s") {
    vsm.requireColKeyString("export plink")

    val ec = EvalContext(Map(
      "s" -> (0, TString()),
      "sa" -> (1, vsm.saSignature),
      "global" -> (2, vsm.globalSignature)))

    ec.set(2, vsm.globalAnnotation)

    type Formatter = (Option[Any]) => String

    val formatID: Formatter = _.map(_.asInstanceOf[String]).getOrElse("0")
    val formatIsFemale: Formatter = _.map { a =>
      if (a.asInstanceOf[Boolean])
        "2"
      else
        "1"
    }.getOrElse("0")
    val formatIsCase: Formatter = _.map { a =>
      if (a.asInstanceOf[Boolean])
        "2"
      else
        "1"
    }.getOrElse("-9")
    val formatQPheno: Formatter = a => a.map(_.toString).getOrElse("-9")

    val famColumns: Map[String, (Type, Int, Formatter)] = Map(
      "famID" -> (TString(), 0, formatID),
      "id" -> (TString(), 1, formatID),
      "patID" -> (TString(), 2, formatID),
      "matID" -> (TString(), 3, formatID),
      "isFemale" -> (TBoolean(), 4, formatIsFemale),
      "qPheno" -> (TFloat64(), 5, formatQPheno),
      "isCase" -> (TBoolean(), 5, formatIsCase))

    val (names, types, f) = Parser.parseNamedExprs(famExpr, ec)

    val famFns: Array[(Array[Option[Any]]) => String] = Array(
      _ => "0", _ => "0", _ => "0", _ => "0", _ => "-9", _ => "-9")

    (names.zipWithIndex, types).zipped.foreach { case ((name, i), t) =>
      famColumns.get(name) match {
        case Some((colt, j, formatter)) =>
          if (colt != t)
            fatal(s"invalid type for .fam file column $i: expected $colt, got $t")
          famFns(j) = (a: Array[Option[Any]]) => formatter(a(i))

        case None =>
          fatal(s"no .fam file column $name")
      }
    }

    val spaceRegex = """\s+""".r
    val badSampleIds = vsm.stringSampleIds.filter(id => spaceRegex.findFirstIn(id).isDefined)
    if (badSampleIds.nonEmpty) {
      fatal(
        s"""Found ${ badSampleIds.length } sample IDs with whitespace
           |  Please run `renamesamples' to fix this problem before exporting to plink format
           |  Bad sample IDs: @1 """.stripMargin, badSampleIds)
    }

    val bedHeader = Array[Byte](108, 27, 1)

    // FIXME: don't reevaluate the upstream RDD twice
    vsm.rdd2.mapPartitions(
      ExportBedBimFam.bedRowTransformer(vsm.nSamples, vsm.rdd2.typ.rowType)
    ).saveFromByteArrays(path + ".bed", vsm.hc.tmpDir, header = Some(bedHeader))

    vsm.rdd2.mapPartitions(
      ExportBedBimFam.bimRowTransformer(vsm.rdd2.typ.rowType)
    ).writeTable(path + ".bim", vsm.hc.tmpDir)

    val famRows = vsm
      .sampleIdsAndAnnotations
      .map { case (s, sa) =>
        ec.setAll(s, sa)
        val a = f().map(Option(_))
        famFns.map(_ (a)).mkString("\t")
      }

    vsm.hc.hadoopConf.writeTextFile(path + ".fam")(out =>
      famRows.foreach(line => {
        out.write(line)
        out.write("\n")
      }))
  }

  // FIXME move filterAlleles tests to Python and delete
>>>>>>> Moved allele methods to method objects.
  def filterAlleles(filterExpr: String, variantExpr: String = "",
    keep: Boolean = true, subset: Boolean = true, leftAligned: Boolean = false, keepStar: Boolean = false): MatrixTable = {
    if (!vsm.genotypeSignature.isOfType(Genotype.htsGenotypeType))
      fatal(s"filter_alleles: genotype_schema must be the HTS genotype schema, found: ${ vsm.genotypeSignature }")

    val genotypeExpr = if (subset) {
      """
g = let newpl = if (isDefined(g.PL))
        let unnorm = range(newV.nGenotypes).map(newi =>
            let oldi = gtIndex(newToOld[gtj(newi)], newToOld[gtk(newi)])
             in g.PL[oldi]) and
            minpl = unnorm.min()
         in unnorm - minpl
      else
        NA: Array[Int] and
    newgt = gtFromPL(newpl) and
    newad = if (isDefined(g.AD))
        range(newV.nAlleles).map(newi => g.AD[newToOld[newi]])
      else
        NA: Array[Int] and
    newgq = gqFromPL(newpl) and
    newdp = g.DP
 in { GT: Call(newgt), AD: newad, DP: newdp, GQ: newgq, PL: newpl }
        """
    } else {
      // downcode
      s"""
g = let newgt = gtIndex(oldToNew[gtj(g.GT)], oldToNew[gtk(g.GT)]) and
    newad = if (isDefined(g.AD))
        range(newV.nAlleles).map(i => range(v.nAlleles).filter(j => oldToNew[j] == i).map(j => g.AD[j]).sum())
      else
        NA: Array[Int] and
    newdp = g.DP and
    newpl = if (isDefined(g.PL))
        range(newV.nGenotypes).map(gi => range(v.nGenotypes).filter(gj => gtIndex(oldToNew[gtj(gj)], oldToNew[gtk(gj)]) == gi).map(gj => g.PL[gj]).min())
      else
        NA: Array[Int] and
    newgq = gqFromPL(newpl)
 in { GT: Call(newgt), AD: newad, DP: newdp, GQ: newgq, PL: newpl }
        """
    }

    FilterAlleles(vsm, filterExpr, variantExpr, genotypeExpr,
      keep = keep, leftAligned = leftAligned, keepStar = keepStar)
  }

  def hardCalls(): MatrixTable = {
    vsm.annotateGenotypesExpr("g = {GT: g.GT}")
  }

  /**
    *
    * @param mafThreshold     Minimum minor allele frequency threshold
    * @param includePAR       Include pseudoautosomal regions
    * @param fFemaleThreshold Samples are called females if F < femaleThreshold
    * @param fMaleThreshold   Samples are called males if F > maleThreshold
    * @param popFreqExpr      Use an annotation expression for estimate of MAF rather than computing from the data
    */
  def imputeSex(mafThreshold: Double = 0.0, includePAR: Boolean = false, fFemaleThreshold: Double = 0.2,
    fMaleThreshold: Double = 0.8, popFreqExpr: Option[String] = None): MatrixTable = {
    ImputeSexPlink(vsm,
      mafThreshold,
      includePAR,
      fMaleThreshold,
      fFemaleThreshold,
      popFreqExpr)
  }

  def ldMatrix(forceLocal: Boolean = false): LDMatrix = {
    LDMatrix(vsm, Some(forceLocal))
  }

  def nirvana(config: String, blockSize: Int = 500000, root: String): MatrixTable = {
    Nirvana.annotate(vsm, config, blockSize, root)
  }

<<<<<<< 6132801e0afb2ba454a7bfc09ee47f2258ed4232
=======
  def rrm(forceBlock: Boolean = false, forceGramian: Boolean = false): KinshipMatrix = {
    info(s"rrm: Computing Realized Relationship Matrix...")
    val (rrm, m) = ComputeRRM(vsm, forceBlock, forceGramian)
    info(s"rrm: RRM computed using $m variants.")
    KinshipMatrix(vsm.hc, vsm.sSignature, rrm, vsm.sampleIds.toArray, m)
  }

>>>>>>> Moved allele methods to method objects.
  /**
    *
    * @param config    VEP configuration file
    * @param root      Variant annotation path to store VEP output
    * @param csq       Annotates with the VCF CSQ field as a string, rather than the full nested struct schema
    * @param blockSize Variants per VEP invocation
    */
  def vep(config: String, root: String = "va.vep", csq: Boolean = false,
    blockSize: Int = 1000): MatrixTable = {
    VEP.annotate(vsm, config, root, csq, blockSize)
  }

  def filterIntervals(intervals: java.util.ArrayList[Interval[Locus]], keep: Boolean): MatrixTable = {
    implicit val locusOrd = vsm.genomeReference.locusOrdering
    val iList = IntervalTree[Locus](intervals.asScala.toArray)
    filterIntervals(iList, keep)
  }

  def filterIntervals[T, U](iList: IntervalTree[Locus, U], keep: Boolean): MatrixTable = {
    implicit val locusOrd = vsm.matrixType.locusType.ordering(missingGreatest = true)

    val ab = new ArrayBuilder[(Interval[Annotation], Annotation)]()
    iList.foreach { case (i, v) =>
      ab += (Interval[Annotation](i.start, i.end), v)
    }

    val iList2 = IntervalTree.annotationTree(ab.result())

    if (keep)
      vsm.copy(rdd = vsm.rdd.filterIntervals(iList2))
    else {
      val iListBc = vsm.sparkContext.broadcast(iList)
      vsm.filterVariants { (v, va, gs) => !iListBc.value.contains(v.asInstanceOf[Variant].locus) }
    }
  }

<<<<<<< 27fdb031ad9f345ab49757bbd6af736bc2d4b446
  /**
    * Remove multiallelic variants from this dataset.
    *
    * Useful for running methods that require biallelic variants without calling the more expensive split_multi step.
    */
  def filterMulti(): MatrixTable = {
    if (vsm.wasSplit) {
      warn("called redundant `filter_multi' on an already split or multiallelic-filtered VDS")
      vsm
    } else {
      vsm.filterVariants {
        case (v, va, gs) => v.asInstanceOf[Variant].isBiallelic
      }.copy2(wasSplit = true)
    }
  }

  def verifyBiallelic(): MatrixTable =
    verifyBiallelic("verifyBiallelic")

  def verifyBiallelic(method: String): MatrixTable = {
    if (vsm.wasSplit) {
      warn("called redundant `$method' on biallelic VDS")
      vsm
    } else {
      val localRowType = vsm.rowType
      vsm.copy2(
        rdd2 = vsm.rdd2.mapPreservesPartitioning(vsm.rdd2.typ) { rv =>
          val ur = new UnsafeRow(localRowType, rv.region, rv.offset)
          val v = ur.getAs[Variant](1)
          if (!v.isBiallelic)
            fatal("in $method: found non-biallelic variant: $v")
          rv
        },
        wasSplit = true)
    }
  }

  def splitMulti(keepStar: Boolean = false, leftAligned: Boolean = false): MatrixTable = {
    if (!vsm.genotypeSignature.isOfType(Genotype.htsGenotypeType))
      fatal(s"split_multi: genotype_schema must be the HTS genotype schema, found: ${ vsm.genotypeSignature }")

    vsm.splitMultiGeneric("va.aIndex = aIndex, va.wasSplit = wasSplit",
      s"""g =
    let
      newgt = downcode(g.GT, aIndex) and
      newad = if (isDefined(g.AD))
          let sum = g.AD.sum() and adi = g.AD[aIndex] in [sum - adi, adi]
        else
          NA: Array[Int] and
      newpl = if (isDefined(g.PL))
          range(3).map(i => range(g.PL.length).filter(j => downcode(Call(j), aIndex) == Call(i)).map(j => g.PL[j]).min())
        else
          NA: Array[Int] and
      newgq = gqFromPL(newpl)
    in { GT: newgt, AD: newad, DP: g.DP, GQ: newgq, PL: newpl }""",
      keepStar, leftAligned)
  }

  def splitMultiGeneric(variantExpr: String, genotypeExpr: String, keepStar: Boolean = false, leftAligned: Boolean = false): MatrixTable = {
    val splitmulti = new SplitMulti(vsm, variantExpr, genotypeExpr, keepStar, leftAligned)
    splitmulti.split()
=======
  def exportGen(path: String, precision: Int = 4) {
    def writeSampleFile() {
      // FIXME: should output all relevant sample annotations such as phenotype, gender, ...
      vsm.hc.hadoopConf.writeTable(path + ".sample",
        "ID_1 ID_2 missing" :: "0 0 0" :: vsm.sampleIds.map(s => s"$s $s 0").toList)
    }

    def writeGenFile() {
      val varidSignature = vsm.vaSignature.getOption("varid")
      val varidQuery: Querier = varidSignature match {
        case Some(_) =>
          val (t, q) = vsm.queryVA("va.varid")
          t match {
            case _: TString => q
            case _ => a => null
          }
        case None => a => null
      }

      val rsidSignature = vsm.vaSignature.getOption("rsid")
      val rsidQuery: Querier = rsidSignature match {
        case Some(_) =>
          val (t, q) = vsm.queryVA("va.rsid")
          t match {
            case _: TString => q
            case _ => a => null
          }
        case None => a => null
      }

      val localNSamples = vsm.nSamples
      val localRowType = vsm.rowType
      vsm.rdd2.mapPartitions { it =>
        val sb = new StringBuilder
        val view = new ArrayGenotypeView(localRowType)
        it.map { rv =>
          view.setRegion(rv)
          val ur = new UnsafeRow(localRowType, rv)

          val v = ur.getAs[Variant](1)
          val va = ur.get(2)

          sb.clear()
          sb.append(v.contig)
          sb += ' '
          sb.append(Option(varidQuery(va)).getOrElse(v.toString))
          sb += ' '
          sb.append(Option(rsidQuery(va)).getOrElse("."))
          sb += ' '
          sb.append(v.start)
          sb += ' '
          sb.append(v.ref)
          sb += ' '
          sb.append(v.alt)

          var i = 0
          while (i < localNSamples) {
            view.setGenotype(i)
            if (view.hasGP) {
              sb += ' '
              sb.append(formatDouble(view.getGP(0), precision))
              sb += ' '
              sb.append(formatDouble(view.getGP(1), precision))
              sb += ' '
              sb.append(formatDouble(view.getGP(2), precision))
            } else
              sb.append(" 0 0 0")
            i += 1
          }
          sb.result()
        }
      }.writeTable(path + ".gen", vsm.hc.tmpDir, None)
    }

    writeSampleFile()
    writeGenFile()
>>>>>>> Moved allele methods to method objects.
  }
}
