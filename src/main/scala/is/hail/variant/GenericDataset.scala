package is.hail.variant

import java.io.FileNotFoundException

import is.hail.HailContext
import is.hail.annotations._
import is.hail.expr.{EvalContext, JSONAnnotationImpex, Parser, SparkAnnotationImpex, TGenotype, TSample, TString, TStruct, TVariant, Type}
import is.hail.io.vcf.ExportVCF
import is.hail.methods.Filter
import is.hail.sparkextras.{OrderedPartitioner, OrderedRDD}
import is.hail.utils._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}
import org.json4s._
import org.json4s.jackson.{JsonMethods, Serialization}

import scala.collection.mutable

object GenericDataset {
  def read(hc: HailContext, dirname: String,
    skipGenotypes: Boolean = false, skipVariants: Boolean = false): GenericDataset = {

    val sqlContext = hc.sqlContext
    val sc = hc.sc
    val hConf = sc.hadoopConfiguration

    val (metadata, parquetGenotypes) = VariantDataset.readMetadata(hConf, dirname, skipGenotypes)
    val vaSignature = metadata.vaSignature
    val vaRequiresConversion = SparkAnnotationImpex.requiresConversion(vaSignature)

    val genotypeSignature = metadata.genotypeSignature
    val gRequiresConversion = SparkAnnotationImpex.requiresConversion(genotypeSignature)
    val isGenericGenotype = metadata.isGenericGenotype
    
    require(isGenericGenotype && !parquetGenotypes, "Can only read datasets with generic genotypes.")

    val parquetFile = dirname + "/rdd.parquet"

    val orderedRDD = if (skipVariants)
      OrderedRDD.empty[Locus, Variant, (Annotation, SharedIterable[Annotation])](sc)
    else {
      val rdd = if (skipGenotypes)
        sqlContext.readParquetSorted(parquetFile, Some(Array("variant", "annotations")))
          .map(row => (row.getVariant(0),
            (if (vaRequiresConversion) SparkAnnotationImpex.importAnnotation(row.get(1), vaSignature) else row.get(1),
              SharedIterable.empty[Annotation])))
      else {
        val rdd = sqlContext.readParquetSorted(parquetFile)
        rdd.map { row =>
          val v = row.getVariant(0)
          (v,
            (if (vaRequiresConversion) SparkAnnotationImpex.importAnnotation(row.get(1), vaSignature) else row.get(1),
              row.getSeq[Any](2).lazyMapShared { g => if (gRequiresConversion) SparkAnnotationImpex.importAnnotation(g, genotypeSignature) else g }
              )
            )
        }
      }

      val partitioner: OrderedPartitioner[Locus, Variant] =
        try {
          val jv = hConf.readFile(dirname + "/partitioner.json.gz")(JsonMethods.parse(_))
          jv.fromJSON[OrderedPartitioner[Locus, Variant]]
        } catch {
          case _: FileNotFoundException =>
            fatal("missing partitioner.json.gz when loading VDS, create with HailContext.write_partitioning.")
        }

      OrderedRDD(rdd, partitioner)
    }

    new VariantSampleMatrix[Annotation](hc,
      if (skipGenotypes) metadata.copy(sampleIds = IndexedSeq.empty[String],
        sampleAnnotations = IndexedSeq.empty[Annotation])
      else metadata,
      orderedRDD)
  }
}

class GenericDatasetFunctions(private val gds: VariantSampleMatrix[Annotation]) extends AnyVal {

  def annotateGenotypesExpr(expr: String): GenericDataset = {
    val symTab = Map(
      "v" -> (0, TVariant),
      "va" -> (1, gds.vaSignature),
      "s" -> (2, TSample),
      "sa" -> (3, gds.saSignature),
      "g" -> (4, gds.genotypeSignature),
      "global" -> (5, gds.globalSignature))


    val ec = EvalContext(symTab)
    ec.set(5, gds.globalAnnotation)

    val (paths, types, f) = Parser.parseAnnotationExprs(expr, ec, Some(Annotation.GENOTYPE_HEAD))

    val inserterBuilder = mutable.ArrayBuilder.make[Inserter]
    val finalType = (paths, types).zipped.foldLeft(gds.genotypeSignature) { case (gsig, (ids, signature)) =>
      val (s, i) = gsig.insert(signature, ids)
      inserterBuilder += i
      s
    }
    val inserters = inserterBuilder.result()

    gds.mapValuesWithAll(
      (v: Variant, va: Annotation, s: String, sa: Annotation, g: Annotation) => {
        ec.setAll(v, va, s, sa, g)
        f().zip(inserters)
          .foldLeft(g) { case (ga, (a, inserter)) =>
            inserter(ga, a)
          }
      }).copy(genotypeSignature = finalType)
  }

  /**
    *
    * @param path output path
    * @param append append file to header
    * @param exportPP export Hail PLs as a PP format field
    * @param parallel export VCF in parallel using the path argument as a directory
    */
  def exportVCF(path: String, append: Option[String] = None, exportPP: Boolean = false, parallel: Boolean = false) {
    ExportVCF(gds, path, append, exportPP, parallel)
  }

  /**
    *
    * @param filterExpr filter expression involving v (Variant), va (variant annotations), s (sample),
    *                   sa (sample annotations), and g (genotype annotation), which returns a boolean value
    * @param keep keep genotypes where filterExpr evaluates to true
    */
  def filterGenotypes(filterExpr: String, keep: Boolean = true): GenericDataset = {

    val symTab = Map(
      "v" -> (0, TVariant),
      "va" -> (1, gds.vaSignature),
      "s" -> (2, TSample),
      "sa" -> (3, gds.saSignature),
      "g" -> (4, gds.genotypeSignature),
      "global" -> (5, gds.globalSignature))


    val ec = EvalContext(symTab)
    ec.set(5, gds.globalAnnotation)
    val f: () => java.lang.Boolean = Parser.parseTypedExpr[java.lang.Boolean](filterExpr, ec)

    val localKeep = keep
    gds.mapValuesWithAll(
      (v: Variant, va: Annotation, s: String, sa: Annotation, g: Annotation) => {
        ec.setAll(v, va, s, sa, g)

        if (Filter.boxedKeepThis(f(), localKeep))
          g
        else
          null
      })
  }

  def queryGA(code: String): (Type, Querier) = {

    val st = Map(Annotation.GENOTYPE_HEAD -> (0, gds.genotypeSignature))
    val ec = EvalContext(st)
    val a = ec.a

    val (t, f) = Parser.parseExpr(code, ec)

    val f2: Annotation => Any = { annotation =>
      a(0) = annotation
      f()
    }

    (t, f2)
  }

  def toVDS: VariantDataset = {
    if (gds.genotypeSignature != TGenotype)
      fatal(s"Cannot convert a GDS to a VDS with signature `${ gds.genotypeSignature.toPrettyString() }'")

    gds.mapValues(a => a.asInstanceOf[Genotype]).copy(isGenericGenotype = false)
  }

  def write(dirname: String, overwrite: Boolean = false): Unit = {
    require(dirname.endsWith(".vds"), "generic dataset write paths must end in '.vds'")
    require(gds.isGenericGenotype, "Can only write datasets with generic genotypes.")

    if (overwrite)
      gds.hadoopConf.delete(dirname, recursive = true)
    else if (gds.hadoopConf.exists(dirname))
      fatal(s"file already exists at `$dirname'")

    gds.writeMetadata(dirname, parquetGenotypes = false)

    val vaSignature = gds.vaSignature
    val vaRequiresConversion = SparkAnnotationImpex.requiresConversion(vaSignature)

    val genotypeSignature = gds.genotypeSignature
    val gRequiresConversion = SparkAnnotationImpex.requiresConversion(genotypeSignature)

    gds.hadoopConf.writeTextFile(dirname + "/partitioner.json.gz") { out =>
      Serialization.write(gds.rdd.orderedPartitioner.toJSON, out)
    }

    val rowRDD = gds.rdd.map { case (v, (va, gs)) =>
      Row.fromSeq(Array(v.toRow,
        if (vaRequiresConversion) SparkAnnotationImpex.exportAnnotation(va, vaSignature) else va,
        gs.lazyMap { g =>
          if (gRequiresConversion)
            SparkAnnotationImpex.exportAnnotation(g, genotypeSignature)
          else
            g
        }.toIterable.toArray[Any]: IndexedSeq[Any])) // FIXME
    }

    gds.hc.sqlContext.createDataFrame(rowRDD, makeSchema)
      .write.parquet(dirname + "/rdd.parquet")
  }

  def makeSchema: StructType = {
    StructType(Array(
      StructField("variant", Variant.schema, nullable = false),
      StructField("annotations", gds.vaSignature.schema),
      StructField("gs", ArrayType(gds.genotypeSignature.schema))
    ))
  }

}
