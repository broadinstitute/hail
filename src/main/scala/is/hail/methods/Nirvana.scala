package is.hail.methods

import java.io.{FileInputStream, IOException}
import java.util.Properties

import is.hail.annotations.{Annotation, Querier}
import is.hail.expr.{JSONAnnotationImpex, Parser, TArray, TBoolean, TDouble, TInt, TSet, TString, TStruct, Type}
import is.hail.utils._
import is.hail.variant.{Variant, VariantDataset}
import org.apache.spark.storage.StorageLevel
import org.json4s.jackson.JsonMethods

import scala.collection.JavaConverters._


object Nirvana {

  // Originally the schema exactly matched Nirvana's JSON output, but in the interest of
  // speed and avoiding redundancy I've removed several fields that would be determined
  // from parsing VCF INFO fields. They are commented out and labeled as such below.


  //NOTE THIS SCHEMA IS FOR NIRVANA 1.6.2 as of JUNE 19th
  val nirvanaSignature = TStruct(
    "chromosome" -> TString,
    "refAllele" -> TString,
    "position" -> TInt,
    "altAlleles" -> TArray(TString),
    "cytogeneticBand" -> TString,
    //"quality" -> TDouble,                 //Derived from QUAL, leaving out
    "filters" -> TArray(TString),
    //"jointSomaticNormalQuality" -> TInt,  //Derived from INFO, leaving out
    //"copyNumber" -> TInt,                 //Derived from INFO, leaving out
    //"strandBias" -> TDouble,              //Derived from INFO, leaving out
    //"recalibratedQuality" -> TDouble,     //Derived from INFO, leaving out
    "variants" -> TArray(TStruct(
      "altAllele" -> TString,
      "refAllele" -> TString,
      "chromosome" -> TString,
      "begin" -> TInt,
      "end" -> TInt,
      "phylopScore" -> TDouble,
      "isReferenceMinor" -> TBoolean,
      "variantType" -> TString,
      "vid" -> TString,
      "isRecomposed" -> TBoolean,
      "regulatoryRegions" -> TArray(TStruct(
        "id" -> TString,
        "consequence" -> TSet(TString),
        "type" -> TString
      )),
      "clinvar" -> TArray(TStruct(
        "id" -> TString,
        "reviewStatus" -> TString,
        "isAlleleSpecific" -> TBoolean,
        "alleleOrigins" -> TArray(TString),
        "refAllele" -> TString,
        "altAllele" -> TString,
        "phenotypes" -> TArray(TString),
        "medGenIds" -> TArray(TString),
        "omimIds" -> TArray(TString),
        "orphanetIds" -> TArray(TString),
        "geneReviewsId" -> TString,
        "significance" -> TString,
        "lastUpdatedDate" -> TString,
        "pubMedIds" -> TArray(TString)
      )),
      "cosmic" -> TArray(TStruct(
        "id" -> TString,
        "isAlleleSpecific" -> TBoolean,
        "refAllele" -> TString,
        "altAllele" -> TString,
        "gene" -> TString,
        "sampleCount" -> TInt,
        "studies" -> TArray(TStruct(
          "id" -> TInt,
          "histology" -> TString,
          "primarySite" -> TString
        ))
      )),
      "dbsnp" -> TStruct("ids" -> TArray(TString)),
      "evs" -> TStruct(
        "coverage" -> TInt,
        "sampleCount" -> TInt,
        "allAf" -> TDouble,
        "afrAf" -> TDouble,
        "eurAf" -> TDouble
      ),
      "exac" -> TStruct(
        "coverage" -> TInt,
        "allAf" -> TDouble,
        "allAc" -> TInt,
        "allAn" -> TInt,
        "afrAf" -> TDouble,
        "afrAc" -> TInt,
        "afrAn" -> TInt,
        "amrAf" -> TDouble,
        "amrAc" -> TInt,
        "amrAn" -> TInt,
        "easAf" -> TDouble,
        "easAc" -> TInt,
        "easAn" -> TInt,
        "finAf" -> TDouble,
        "finAc" -> TInt,
        "finAn" -> TInt,
        "nfeAf" -> TDouble,
        "nfeAc" -> TInt,
        "nfeAn" -> TInt,
        "othAf" -> TDouble,
        "othAc" -> TInt,
        "othAn" -> TInt,
        "sasAf" -> TDouble,
        "sasAc" -> TInt,
        "sasAn" -> TInt
      ),
      "globalAllele" -> TStruct(
        "globalMinorAllele" -> TString,
        "globalMinorAlleleFrequency" -> TDouble
      ),
      "oneKg" -> TStruct(
        "ancestralAllele" -> TString,
        "allAf" -> TDouble,
        "allAc" -> TInt,
        "allAn" -> TInt,
        "afrAf" -> TDouble,
        "afrAc" -> TInt,
        "afrAn" -> TInt,
        "amrAf" -> TDouble,
        "amrAc" -> TInt,
        "amrAn" -> TInt,
        "easAf" -> TDouble,
        "easAc" -> TInt,
        "easAn" -> TInt,
        "eurAf" -> TDouble,
        "eurAc" -> TInt,
        "eurAn" -> TInt,
        "sasAf" -> TDouble,
        "sasAc" -> TInt,
        "sasAn" -> TInt
      ),
      "transcripts" -> TStruct(
        "refSeq" -> TArray(TStruct(
          "transcript" -> TString,
          "bioType" -> TString,
          "aminoAcids" -> TString,
          "cDnaPos" -> TString,
          "codons" -> TString,
          "cdsPos" -> TString,
          "exons" -> TString,
          "introns" -> TString,
          "geneId" -> TString,
          "hgnc" -> TString,
          "consequence" -> TArray(TString),
          "hgvsc" -> TString,
          "hgvsp" -> TString,
          "isCanonical" -> TBoolean,
          "polyPhenScore" -> TDouble,
          "polyPhenPrediction" -> TString,
          "proteinId" -> TString,
          "proteinPos" -> TString,
          "siftScore" -> TDouble,
          "siftPrediction" -> TString
        )),
        "ensembl" -> TArray(TStruct(
          "transcript" -> TString,
          "bioType" -> TString,
          "aminoAcids" -> TString,
          "cDnaPos" -> TString,
          "codons" -> TString,
          "cdsPos" -> TString,
          "exons" -> TString,
          "introns" -> TString,
          "geneId" -> TString,
          "hgnc" -> TString,
          "consequence" -> TArray(TString),
          "hgvsc" -> TString,
          "hgvsp" -> TString,
          "isCanonical" -> TBoolean,
          "polyPhenScore" -> TDouble,
          "polyPhenPrediction" -> TString,
          "proteinId" -> TString,
          "proteinPos" -> TString,
          "siftScore" -> TDouble,
          "siftPrediction" -> TString
        ))
      ),
      "genes" -> TArray(TStruct(
        "name" -> TString,
        "omim" -> TArray(TStruct(
          "mimNumber" -> TInt,
          "hgnc" -> TString,
          "description" -> TString,
          "phenotypes" -> TArray(TStruct(
            "mimNumber" -> TInt,
            "phenotype" -> TString,
            "mapping" -> TString,
            "inheritance" -> TArray(TString),
            "comments" -> TString
          ))
        ))
      ))
    ))
  )

  def printContext(w: (String) => Unit) {
    w("##fileformat=VCFv4.1")
    w("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT")
  }

  def printElement(vaSignature: Type)(w: (String) => Unit, v: Variant) {
    val sb = new StringBuilder()
    sb.append(v.contig)
    sb += '\t'
    sb.append(v.start)
    sb.append("\t.\t")
    sb.append(v.ref)
    sb += '\t'
    sb.append(v.altAlleles.iterator.map(_.alt).mkString(","))
    sb += '\t'
    sb.append("\t.\t.\tGT")
    w(sb.result())
  }

  def variantFromInput(contig: String, start: Int, ref: String, altAlleles: Array[String]): Variant = {
    Variant(contig, start, ref, altAlleles)
  }

  def annotate(vds: VariantDataset, config: String, blockSize: Int, root: String = "va.nirvana"): VariantDataset = {
    val parsedRoot = Parser.parseAnnotationRoot(root, Annotation.VARIANT_HEAD)

    val rootType =
      vds.vaSignature.getOption(parsedRoot)
        .filter { t =>
          val r = t == nirvanaSignature
          if (!r) {
            warn(s"type for $parsedRoot does not match Nirvana signature, overwriting.")
          }
          r
        }

    val properties = try {
      val p = new Properties()
      val is = new FileInputStream(config)
      p.load(is)
      is.close()
      p
    } catch {
      case e: IOException =>
        fatal(s"could not open file: ${ e.getMessage }")
    }

    val dotnet = properties.getProperty("hail.nirvana.dotnet", "dotnet")

    val nirvanaLocation = properties.getProperty("hail.nirvana.location")
    if (nirvanaLocation == null)
      fatal("property `hail.nirvana.location' required")

    val path = Option(properties.getProperty("hail.nirvana.path"))

    val cache = properties.getProperty("hail.nirvana.cache")


    val supplementaryAnnotationDirectoryOpt = Option(properties.getProperty("hail.nirvana.supplementaryAnnotationDirectory"))
    val supplementaryAnnotationDirectory = if(supplementaryAnnotationDirectoryOpt.isEmpty) List[String]() else List("--sd", supplementaryAnnotationDirectoryOpt.get)

    val reference = properties.getProperty("hail.nirvana.reference")

    val rootQuery = rootType
      .map(_ => vds.vaSignature.query(parsedRoot))

    val cmd: List[String] = List[String](dotnet, s"$nirvanaLocation") ++
      List("-c", cache) ++
      supplementaryAnnotationDirectory ++
      List("-r", reference,
      "-i", "-",
      "-o", "-")

    println(cmd.mkString(" "))

    val contigQuery: Querier = nirvanaSignature.query("chromosome")
    val startQuery = nirvanaSignature.query("position")
    val refQuery = nirvanaSignature.query("refAllele")
    val altsQuery = nirvanaSignature.query("altAlleles")
    val oldSignature = vds.vaSignature
    val localBlockSize = blockSize

    info("Running Nirvana")

    val annotations = vds.rdd.mapValues { case (va, gs) => va }
      .mapPartitions({ it =>
        val pb = new ProcessBuilder(cmd.asJava)
        val env = pb.environment()
        if (path.getOrElse(null) != null)
          env.put("PATH", path.get)

        it.filter { case (v, va) =>
          rootQuery.forall(q => q(va) == null)
        }
          .map { case (v, _) => v }
          .grouped(localBlockSize)
          .flatMap { block =>
            val (jt, proc) = block.iterator.pipe(pb,
              printContext,
              printElement(oldSignature),
              _ => ())
            //The drop is to ignore the header line, the filter is because every other output line is a comma.
            val kt = jt.filter(_.startsWith("{\"chromosome")).map { s =>
                val a = JSONAnnotationImpex.importAnnotation(JsonMethods.parse(s), nirvanaSignature)
                if(startQuery(a).asInstanceOf[Int] == 0) {
                  println()
                  println(s)
                  println(a)
                }
                val v = variantFromInput(contigQuery(a).asInstanceOf[String],
                  startQuery(a).asInstanceOf[Int],
                  refQuery(a).asInstanceOf[String],
                  altsQuery(a).asInstanceOf[Seq[String]].toArray
                )
                (v, a)
              }

            val r = kt.toArray
              .sortBy(_._1)

            val rc = proc.waitFor()
            if (rc != 0)
              fatal(s"nirvana command failed with non-zero exit status $rc")

            r
          }
      }, preservesPartitioning = true)
      .persist(StorageLevel.MEMORY_AND_DISK)


    info(s"nirvana: annotated ${ annotations.count() } variants")

    val (newVASignature, insertNirvana) = vds.vaSignature.insert(nirvanaSignature, parsedRoot)

    val newRDD = vds.rdd
      .zipPartitions(annotations, preservesPartitioning = true) { case (left, right) =>
        left.sortedLeftJoinDistinct(right)
          .map { case (v, ((va, gs), vaNirvana)) =>
            (v, (insertNirvana(va, vaNirvana.orNull), gs))
          }
      }.asOrderedRDD

    vds.copy(rdd = newRDD,
      vaSignature = newVASignature)
  }

}
