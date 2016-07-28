package org.broadinstitute.hail.variant

import java.nio.ByteBuffer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkContext, SparkEnv}
import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.annotations._
import org.broadinstitute.hail.check.Gen
import org.broadinstitute.hail.expr._
import org.broadinstitute.hail.utils.{Interval, IntervalTree}
import org.broadinstitute.hail.vcf.BufferedLineIterator
import org.kududb.spark.kudu.{KuduContext, _}

import scala.io.Source
import scala.language.implicitConversions
import scala.reflect.ClassTag

object VariantSampleMatrix {
  final val magicNumber: Int = 0xe51e2c58
  final val fileVersion: Int = 3

  def apply[T](metadata: VariantMetadata,
    rdd: RDD[(Variant, Annotation, Iterable[T])])(implicit tct: ClassTag[T]): VariantSampleMatrix[T] = {
    new VariantSampleMatrix(metadata, rdd)
  }

  private def readMetadata(sqlContext: SQLContext, dirname: String, skipGenotypes: Boolean = false,
    requireParquetSuccess: Boolean = true): VariantMetadata = {
    if (!dirname.endsWith(".vds") && !dirname.endsWith(".vds/"))
      fatal(s"input path ending in `.vds' required, found `$dirname'")

    val hConf = sqlContext.sparkContext.hadoopConfiguration

    if (!hadoopExists(hConf, dirname))
      fatal(s"no VDS found at `$dirname'")

    val vaSchema = dirname + "/va.schema"
    val saSchema = dirname + "/sa.schema"
    val globalSchema = dirname + "/global.schema"
    val pqtSuccess = dirname + "/rdd.parquet/_SUCCESS"
    val metadataFile = dirname + "/metadata.ser"

    if (!hadoopExists(hConf, metadataFile))
      fatal("corrupt VDS: no metadata.ser file.  Recreate VDS.")

    val (sampleIds, sampleAnnotations, globalAnnotation, wasSplit) =
      readDataFile(dirname + "/metadata.ser", hConf) { dis =>
        try {
          val serializer = SparkEnv.get.serializer.newInstance()
          val ds = serializer.deserializeStream(dis)

          val m = ds.readObject[Int]
          if (m != magicNumber)
            fatal("Invalid VDS: invalid magic number")

          val v = ds.readObject[Int]
          if (v != fileVersion)
            fatal("Old VDS version found")

          val sampleIds = ds.readObject[IndexedSeq[String]]
          val sampleAnnotations = ds.readObject[IndexedSeq[Annotation]]
          val globalAnnotation = ds.readObject[Annotation]
          val wasSplit = ds.readObject[Boolean]

          ds.close()
          (sampleIds, sampleAnnotations, globalAnnotation, wasSplit)

        } catch {
          case e: Exception =>
            println(e)
            fatal(s"Invalid VDS: ${e.getMessage}\n  Recreate with current version of Hail.")
        }
      }

    if (requireParquetSuccess && !hadoopExists(hConf, pqtSuccess))
      fatal("corrupt VDS: no parquet success indicator, meaning a problem occurred during write.  Recreate VDS.")

    if (!hadoopExists(hConf, vaSchema, saSchema, globalSchema))
      fatal("corrupt VDS: one or more .schema files missing.  Recreate VDS.")

    val vaSignature = readFile(dirname + "/va.schema", hConf) { dis =>
      val schema = Source.fromInputStream(dis)
        .mkString
      Parser.parseType(schema)
    }

    val saSignature = readFile(dirname + "/sa.schema", hConf) { dis =>
      val schema = Source.fromInputStream(dis)
        .mkString
      Parser.parseType(schema)
    }

    val globalSignature = readFile(dirname + "/global.schema", hConf) { dis =>
      val schema = Source.fromInputStream(dis)
        .mkString
      Parser.parseType(schema)
    }


    VariantMetadata(sampleIds, sampleAnnotations, globalAnnotation,
      saSignature, vaSignature, globalSignature, wasSplit)
  }

  def read(sqlContext: SQLContext, dirname: String, skipGenotypes: Boolean = false): VariantDataset = {

    val metadata = readMetadata(sqlContext, dirname, skipGenotypes)
    val vaSignature = metadata.vaSignature

    val df = sqlContext.read.parquet(dirname + "/rdd.parquet")

    val vaRequiresConversion = SparkAnnotationImpex.requiresConversion(vaSignature)

    if (skipGenotypes)
      new VariantSampleMatrix[Genotype](
        metadata.copy(sampleIds = IndexedSeq.empty[String],
          sampleAnnotations = IndexedSeq.empty[Annotation]),
        df.select("variant", "annotations")
          .map(row => (row.getVariant(0),
            if (vaRequiresConversion) SparkAnnotationImpex.importAnnotation(row.get(1), vaSignature) else row.get(1),
            Iterable.empty[Genotype])))
    else
      new VariantSampleMatrix(
        metadata,
        df.rdd.map { row =>
          val v = row.getVariant(0)

          (v,
            if (vaRequiresConversion) SparkAnnotationImpex.importAnnotation(row.get(1), vaSignature) else row.get(1),
            row.getGenotypeStream(v, 2))
        })
  }

  def kuduRowType(vaSignature: Type): Type = TStruct("variant" -> Variant.t,
    "annotations" -> vaSignature,
    "gs" -> GenotypeStream.t,
    "sample_group" -> TString)

  def readKudu(sqlContext: SQLContext, dirname: String, tableName: String,
    master: String): VariantDataset = {

    val metadata = readMetadata(sqlContext, dirname, skipGenotypes = false,
      requireParquetSuccess = false)
    val vaSignature = metadata.vaSignature

    val df = sqlContext.read.options(
      Map("kudu.table" -> tableName, "kudu.master" -> master)).kudu

    val rowType = kuduRowType(vaSignature)
    val schema: StructType = KuduAnnotationImpex.exportType(rowType).asInstanceOf[StructType]

    // Kudu key fields are always first, so we have to reorder the fields we get back
    // to be in the column order for the flattened schema *before* we unflatten
    val indices: Array[Int] = schema.fields.zipWithIndex.map { case (field, rowIdx) =>
      df.schema.fieldIndex(field.name)
    }

    val rdd: RDD[(Variant, Annotation, Iterable[Genotype])] = df.map { row =>
      val importedRow = KuduAnnotationImpex.importAnnotation(
        KuduAnnotationImpex.reorder(row, indices), rowType).asInstanceOf[Row]
      val v = importedRow.getVariant(0)
      (v,
        (importedRow.get(1),
          importedRow.getGenotypeStream(v, 2)))
    }.spanByKey().map(kv => {
      // combine variant rows with different sample groups (no shuffle)
      val variant = kv._1
      val annotations = kv._2.head._1 // just use first annotation
      val genotypes = kv._2.flatMap(_._2) // combine genotype streams
      (variant, annotations, genotypes)
    })
    new VariantSampleMatrix[Genotype](metadata, rdd)
  }

  private def makeSchemaForKudu(vaSignature: Type): StructType =
    StructType(Array(
      StructField("variant", Variant.schema, nullable = false),
      StructField("annotations", vaSignature.schema, nullable = false),
      StructField("gs", GenotypeStream.schema, nullable = false),
      StructField("sample_group", StringType, nullable = false)
    ))

  def gen[T](sc: SparkContext,
    gen: VSMSubgen[T])(implicit tct: ClassTag[T]): Gen[VariantSampleMatrix[T]] =
    gen.gen(sc)
}

case class VSMSubgen[T](
  sampleIdGen: Gen[IndexedSeq[String]],
  saSigGen: Gen[Type],
  vaSigGen: Gen[Type],
  globalSigGen: Gen[Type],
  saGen: (Int, Type) => Gen[IndexedSeq[Annotation]],
  vaGen: (Type) => Gen[Annotation],
  globalGen: (Type) => Gen[Annotation],
  vGen: Gen[Variant],
  tGen: (Variant) => Gen[T]) {

  def gen(sc: SparkContext)(implicit tct: ClassTag[T]): Gen[VariantSampleMatrix[T]] =
    for (vaSig <- vaSigGen;
      saSig <- saSigGen;
      globalSig <- globalSigGen;
      sampleIds <- sampleIdGen;
      global <- globalGen(globalSig);
      saValues <- saGen(sampleIds.length, saSig);
      rows <- Gen.distinctBuildableOf[Seq[(Variant, Annotation, Iterable[T])], (Variant, Annotation, Iterable[T])](
        for (v <- vGen;
          va <- vaGen(vaSig);
          ts <- Gen.buildableOfN[Iterable[T], T](sampleIds.length, tGen(v)))
          yield (v, va, ts)))
      yield VariantSampleMatrix[T](VariantMetadata(sampleIds, saValues, global, saSig, vaSig, globalSig), sc.parallelize(rows))
}

object VSMSubgen {
  val random = VSMSubgen[Genotype](
    sampleIdGen = Gen.distinctBuildableOf[IndexedSeq[String], String](Gen.identifier),
    saSigGen = Type.genArb,
    vaSigGen = Type.genArb,
    globalSigGen = Type.genArb,
    saGen = (nSamples: Int, t: Type) =>
      Gen.sequence[IndexedSeq[Annotation], Annotation](IndexedSeq.fill[Gen[Annotation]](nSamples)(t.genValue)),
    vaGen = (t: Type) => t.genValue,
    globalGen = (t: Type) => t.genValue,
    vGen = Variant.gen,
    tGen = Genotype.gen)

  val realistic = random.copy(
    tGen = Genotype.genRealistic)
}

class VariantSampleMatrix[T](val metadata: VariantMetadata,
  val rdd: RDD[(Variant, Annotation, Iterable[T])])
  (implicit tct: ClassTag[T]) {

  def sampleIds: IndexedSeq[String] = metadata.sampleIds

  lazy val sampleIdsBc = sparkContext.broadcast(sampleIds)

  def nSamples: Int = metadata.sampleIds.length

  def vaSignature: Type = metadata.vaSignature

  def saSignature: Type = metadata.saSignature

  def globalSignature: Type = metadata.globalSignature

  def globalAnnotation: Annotation = metadata.globalAnnotation

  def sampleAnnotations: IndexedSeq[Annotation] = metadata.sampleAnnotations

  def sampleIdsAndAnnotations: IndexedSeq[(String, Annotation)] = sampleIds.zip(sampleAnnotations)

  lazy val sampleAnnotationsBc = sparkContext.broadcast(sampleAnnotations)

  def wasSplit: Boolean = metadata.wasSplit

  def copy[U](rdd: RDD[(Variant, Annotation, Iterable[U])] = rdd,
    sampleIds: IndexedSeq[String] = sampleIds,
    sampleAnnotations: IndexedSeq[Annotation] = sampleAnnotations,
    globalAnnotation: Annotation = globalAnnotation,
    saSignature: Type = saSignature,
    vaSignature: Type = vaSignature,
    globalSignature: Type = globalSignature,
    wasSplit: Boolean = wasSplit)
    (implicit tct: ClassTag[U]): VariantSampleMatrix[U] =
    new VariantSampleMatrix[U](
      VariantMetadata(sampleIds, sampleAnnotations, globalAnnotation,
        saSignature, vaSignature, globalSignature, wasSplit), rdd)

  def sparkContext: SparkContext = rdd.sparkContext

  def cache(): VariantSampleMatrix[T] = copy[T](rdd = rdd.cache())

  def repartition(nPartitions: Int) = copy[T](rdd = rdd.repartition(nPartitions)(null))

  def nPartitions: Int = rdd.partitions.length

  def variants: RDD[Variant] = rdd.map(_._1)

  def variantsAndAnnotations: RDD[(Variant, Annotation)] = rdd.map { case (v, va, gs) => (v, va) }

  def nVariants: Long = variants.count()

  def expand(): RDD[(Variant, String, T)] =
    mapWithKeys[(Variant, String, T)]((v, s, g) => (v, s, g))

  def expandWithAll(): RDD[(Variant, Annotation, String, Annotation, T)] =
    mapWithAll[(Variant, Annotation, String, Annotation, T)]((v, va, s, sa, g) => (v, va, s, sa, g))

  def sampleVariants(fraction: Double): VariantSampleMatrix[T] =
    copy(rdd = rdd.sample(withReplacement = false, fraction, 1))

  def mapValues[U](f: (T) => U)(implicit uct: ClassTag[U]): VariantSampleMatrix[U] = {
    mapValuesWithAll((v, va, s, sa, g) => f(g))
  }

  def mapValuesWithKeys[U](f: (Variant, String, T) => U)
    (implicit uct: ClassTag[U]): VariantSampleMatrix[U] = {
    mapValuesWithAll((v, va, s, sa, g) => f(v, s, g))
  }

  def mapValuesWithAll[U](f: (Variant, Annotation, String, Annotation, T) => U)
    (implicit uct: ClassTag[U]): VariantSampleMatrix[U] = {
    val localSampleIdsBc = sampleIdsBc
    val localSampleAnnotationsBc = sampleAnnotationsBc
    copy(rdd = rdd.map { case (v, va, gs) =>
      (v, va, localSampleIdsBc.value.lazyMapWith2[Annotation, T, U](localSampleAnnotationsBc.value, gs, {
        case (s, sa, g) => f(v, va, s, sa, g)
      }))
    })
  }

  def mapValuesWithPartialApplication[U](f: (Variant, Annotation) => ((String, Annotation) => U))
    (implicit uct: ClassTag[U]): VariantSampleMatrix[U] = {
    val localSampleIdsBc = sampleIdsBc
    val localSampleAnnotationsBc = sampleAnnotationsBc

    copy(rdd =
      rdd.mapPartitions[(Variant, Annotation, Iterable[U])] { it: Iterator[(Variant, Annotation, Iterable[T])] =>
        it.map { case (v, va, gs) =>
          val f2 = f(v, va)
          (v, va, localSampleIdsBc.value.lazyMapWith2[Annotation, T, U](localSampleAnnotationsBc.value, gs, {
            case (s, sa, g) => f2(s, sa)
          }))
        }
      })
  }

  def map[U](f: T => U)(implicit uct: ClassTag[U]): RDD[U] =
    mapWithKeys((v, s, g) => f(g))

  def mapWithKeys[U](f: (Variant, String, T) => U)(implicit uct: ClassTag[U]): RDD[U] = {
    val localSampleIdsBc = sampleIdsBc

    rdd
      .flatMap { case (v, va, gs) =>
        localSampleIdsBc.value.lazyMapWith[T, U](gs,
          (s, g) => f(v, s, g))
      }
  }

  def mapWithAll[U](f: (Variant, Annotation, String, Annotation, T) => U)(implicit uct: ClassTag[U]): RDD[U] = {
    val localSampleIdsBc = sampleIdsBc
    val localSampleAnnotationsBc = sampleAnnotationsBc

    rdd
      .flatMap { case (v, va, gs) =>
        localSampleIdsBc.value.lazyMapWith2[Annotation, T, U](localSampleAnnotationsBc.value, gs, {
          case (s, sa, g) => f(v, va, s, sa, g)
        })
      }
  }

  def mapPartitionsWithAll[U](f: Iterator[(Variant, Annotation, String, Annotation, T)] => Iterator[U])
    (implicit uct: ClassTag[U]): RDD[U] = {
    val localSampleIdsBc = sampleIdsBc
    val localSampleAnnotationsBc = sampleAnnotationsBc

    rdd.mapPartitions { it =>
      f(it.flatMap { case (v, va, gs) =>
        localSampleIdsBc.value.lazyMapWith2[Annotation, T, (Variant, Annotation, String, Annotation, T)](
          localSampleAnnotationsBc.value, gs, { case (s, sa, g) => (v, va, s, sa, g) })
      })
    }
  }

  def mapAnnotations(f: (Variant, Annotation, Iterable[T]) => Annotation): VariantSampleMatrix[T] =
    copy[T](rdd = rdd.map { case (v, va, gs) => (v, f(v, va, gs), gs) })

  def flatMap[U](f: T => TraversableOnce[U])(implicit uct: ClassTag[U]): RDD[U] =
    flatMapWithKeys((v, s, g) => f(g))

  def flatMapWithKeys[U](f: (Variant, String, T) => TraversableOnce[U])(implicit uct: ClassTag[U]): RDD[U] = {
    val localSampleIdsBc = sampleIdsBc

    rdd
      .flatMap { case (v, va, gs) => localSampleIdsBc.value.lazyFlatMapWith(gs,
        (s: String, g: T) => f(v, s, g))
      }
  }

  def filterVariants(p: (Variant, Annotation, Iterable[T]) => Boolean): VariantSampleMatrix[T] =
    copy(rdd = rdd.filter { case (v, va, gs) => p(v, va, gs) })

  def filterIntervals(gis: IntervalTree[Locus], keep: Boolean = true): VariantSampleMatrix[T] = {
    val gisBc = sparkContext.broadcast(gis)
    filterVariants { (v, va, gs) =>
      val inInterval = gisBc.value.contains(v.locus)
      if (keep)
        inInterval
      else
        !inInterval
    }
  }

  def dropSamples(): VariantSampleMatrix[T] =
    copy(sampleIds = IndexedSeq.empty[String],
      sampleAnnotations = IndexedSeq.empty[Annotation],
      rdd = rdd.map { case (v, va, gs) =>
        (v, va, Iterable.empty[T])
      })

  // FIXME see if we can remove broadcasts elsewhere in the code
  def filterSamples(p: (String, Annotation) => Boolean): VariantSampleMatrix[T] = {
    val mask = sampleIdsAndAnnotations.map { case (s, sa) => p(s, sa) }
    val maskBc = sparkContext.broadcast(mask)
    val localtct = tct
    copy[T](sampleIds = sampleIds.zipWithIndex
      .filter { case (s, i) => mask(i) }
      .map(_._1),
      sampleAnnotations = sampleAnnotations.zipWithIndex
        .filter { case (sa, i) => mask(i) }
        .map(_._1),
      rdd = rdd.map { case (v, va, gs) =>
        (v, va, gs.lazyFilterWith(maskBc.value, (g: T, m: Boolean) => m))
      })
  }

  def aggregateBySample[U](zeroValue: U)(
    seqOp: (U, T) => U,
    combOp: (U, U) => U)(implicit uct: ClassTag[U]): RDD[(String, U)] =
    aggregateBySampleWithKeys(zeroValue)((e, v, s, g) => seqOp(e, g), combOp)

  def aggregateBySampleWithKeys[U](zeroValue: U)(
    seqOp: (U, Variant, String, T) => U,
    combOp: (U, U) => U)(implicit uct: ClassTag[U]): RDD[(String, U)] = {
    aggregateBySampleWithAll(zeroValue)((e, v, va, s, sa, g) => seqOp(e, v, s, g), combOp)
  }

  def aggregateBySampleWithAll[U](zeroValue: U)(
    seqOp: (U, Variant, Annotation, String, Annotation, T) => U,
    combOp: (U, U) => U)(implicit uct: ClassTag[U]): RDD[(String, U)] = {

    val serializer = SparkEnv.get.serializer.newInstance()
    val zeroBuffer = serializer.serialize(zeroValue)
    val zeroArray = new Array[Byte](zeroBuffer.limit)
    zeroBuffer.get(zeroArray)
    val localSampleIdsBc = sampleIdsBc
    val localSampleAnnotationsBc = sampleAnnotationsBc

    rdd
      .mapPartitions { (it: Iterator[(Variant, Annotation, Iterable[T])]) =>
        val serializer = SparkEnv.get.serializer.newInstance()
        def copyZeroValue() = serializer.deserialize[U](ByteBuffer.wrap(zeroArray))
        val arrayZeroValue = Array.fill[U](localSampleIdsBc.value.length)(copyZeroValue())

        localSampleIdsBc.value.iterator
          .zip(it.foldLeft(arrayZeroValue) { case (acc, (v, va, gs)) =>
            for ((g, i) <- gs.iterator.zipWithIndex) {
              acc(i) = seqOp(acc(i), v, va,
                localSampleIdsBc.value(i), localSampleAnnotationsBc.value(i), g)
            }
            acc
          }.iterator)
      }.foldByKey(zeroValue)(combOp)
  }

  def aggregateByVariant[U](zeroValue: U)(
    seqOp: (U, T) => U,
    combOp: (U, U) => U)(implicit uct: ClassTag[U]): RDD[(Variant, U)] =
    aggregateByVariantWithAll(zeroValue)((e, v, va, s, sa, g) => seqOp(e, g), combOp)

  def aggregateByVariantWithKeys[U](zeroValue: U)(
    seqOp: (U, Variant, String, T) => U,
    combOp: (U, U) => U)(implicit uct: ClassTag[U]): RDD[(Variant, U)] = {
    aggregateByVariantWithAll(zeroValue)((e, v, va, s, sa, g) => seqOp(e, v, s, g), combOp)
  }

  def aggregateByVariantWithAll[U](zeroValue: U)(
    seqOp: (U, Variant, Annotation, String, Annotation, T) => U,
    combOp: (U, U) => U)(implicit uct: ClassTag[U]): RDD[(Variant, U)] = {

    // Serialize the zero value to a byte array so that we can apply a new clone of it on each key
    val zeroBuffer = SparkEnv.get.serializer.newInstance().serialize(zeroValue)
    val zeroArray = new Array[Byte](zeroBuffer.limit)
    zeroBuffer.get(zeroArray)

    val localSampleIdsBc = sampleIdsBc
    val localSampleAnnotationsBc = sampleAnnotationsBc

    rdd
      .mapPartitions { (it: Iterator[(Variant, Annotation, Iterable[T])]) =>
        val serializer = SparkEnv.get.serializer.newInstance()
        it.map { case (v, va, gs) =>
          val zeroValue = serializer.deserialize[U](ByteBuffer.wrap(zeroArray))
          (v, gs.iterator.zipWithIndex.map { case (g, i) => (localSampleIdsBc.value(i), localSampleAnnotationsBc.value(i), g) }
            .foldLeft(zeroValue) { case (acc, (s, sa, g)) =>
              seqOp(acc, v, va, s, sa, g)
            })
        }
      }

    /*
        rdd
          .map { case (v, gs) =>
            val serializer = SparkEnv.get.serializer.newInstance()
            val zeroValue = serializer.deserialize[U](ByteBuffer.wrap(zeroArray))

            (v, gs.zipWithIndex.foldLeft(zeroValue) { case (acc, (g, i)) =>
              seqOp(acc, v, localSamplesBc.value(i), g)
            })
          }
    */
  }

  def foldBySample(zeroValue: T)(combOp: (T, T) => T): RDD[(String, T)] = {

    val localtct = tct

    val serializer = SparkEnv.get.serializer.newInstance()
    val zeroBuffer = serializer.serialize(zeroValue)
    val zeroArray = new Array[Byte](zeroBuffer.limit)
    zeroBuffer.get(zeroArray)

    val localSampleIdsBc = sampleIdsBc

    rdd
      .mapPartitions { (it: Iterator[(Variant, Annotation, Iterable[T])]) =>
        val serializer = SparkEnv.get.serializer.newInstance()
        def copyZeroValue() = serializer.deserialize[T](ByteBuffer.wrap(zeroArray))(localtct)
        val arrayZeroValue = Array.fill[T](localSampleIdsBc.value.length)(copyZeroValue())
        localSampleIdsBc.value.iterator
          .zip(it.foldLeft(arrayZeroValue) { case (acc, (v, va, gs)) =>
            for ((g, i) <- gs.iterator.zipWithIndex)
              acc(i) = combOp(acc(i), g)
            acc
          }.iterator)
      }.foldByKey(zeroValue)(combOp)
  }

  def foldByVariant(zeroValue: T)(combOp: (T, T) => T): RDD[(Variant, T)] =
    rdd.map { case (v, va, gs) => (v, gs.foldLeft(zeroValue)((acc, g) => combOp(acc, g))) }

  def same(that: VariantSampleMatrix[T]): Boolean = {
    val metadataSame = metadata == that.metadata
    if (!metadataSame)
      println("metadata were not the same")
    metadataSame &&
      rdd.map { case (v, va, gs) => (v, (va, gs)) }
        .fullOuterJoin(that.rdd.map { case (v, va, gs) => (v, (va, gs)) })
        .forall {
          case (v, (Some((va1, it1)), Some((va2, it2)))) =>
            val annotationsSame = va1 == va2
            if (!annotationsSame)
              println(s"annotations $va1, $va2 were not the same")
            val genotypesSame = (it1, it2).zipped.forall { case (g1, g2) =>
              if (g1 != g2)
                println(s"genotypes $g1, $g2 were not the same")
              g1 == g2
            }
            annotationsSame && genotypesSame
          case (v, _) =>
            println(s"Found unmatched variant $v")
            false
        }
  }

  def mapAnnotationsWithAggregate[U](zeroValue: U, newVAS: Type)(
    seqOp: (U, Variant, Annotation, String, Annotation, T) => U,
    combOp: (U, U) => U,
    mapOp: (Annotation, U) => Annotation)
    (implicit uct: ClassTag[U]): VariantSampleMatrix[T] = {

    // Serialize the zero value to a byte array so that we can apply a new clone of it on each key
    val zeroBuffer = SparkEnv.get.serializer.newInstance().serialize(zeroValue)
    val zeroArray = new Array[Byte](zeroBuffer.limit)
    zeroBuffer.get(zeroArray)

    val localSampleIdsBc = sampleIdsBc
    val localSampleAnnotationsBc = sampleAnnotationsBc

    copy(vaSignature = newVAS,
      rdd = rdd.map {
        case (v, va, gs) =>
          val serializer = SparkEnv.get.serializer.newInstance()
          val zeroValue = serializer.deserialize[U](ByteBuffer.wrap(zeroArray))

          (v, mapOp(va, gs.iterator
            .zip(localSampleIdsBc.value.iterator
              .zip(localSampleAnnotationsBc.value.iterator)).foldLeft(zeroValue) {
            case (acc, (g, (s, sa))) =>
              seqOp(acc, v, va, s, sa, g)
          }), gs)
      })
  }

  def annotateIntervals(is: IntervalTree[Locus],
    arg: Option[(Type, Map[Interval[Locus], Annotation])],
    path: List[String]): VariantSampleMatrix[T] = {

    val isBc = sparkContext.broadcast(is)
    arg match {
      case Some((sig, m)) =>
        val (newSignature, inserter) = insertVA(sig, path)
        val mBc = sparkContext.broadcast(m)
        copy(rdd = rdd.map { case (v, va, gs) =>
          val queries = isBc.value.query(v.locus)
          val toIns = if (queries.isEmpty)
            None
          else
            Some(m(queries.head))
          (v, inserter(va, toIns), gs)
        },
          vaSignature = newSignature)

      case None =>
        val (newSignature, inserter) = insertVA(TBoolean, path)
        copy(rdd = rdd.map { case (v, va, gs) => (v, inserter(va, Some(isBc.value.contains(Locus(v.contig, v.start)))), gs) },
          vaSignature = newSignature)
    }
  }

  def annotateVariants(otherRDD: RDD[(Variant, Annotation)], signature: Type,
    path: List[String]): VariantSampleMatrix[T] = {
    val (newSignature, inserter) = insertVA(signature, path)
    val newRDD = rdd.map { case (v, va, gs) => (v, (va, gs)) }
      .leftOuterJoinDistinct(otherRDD)
      .map { case (v, ((va, gs), annotation)) => (v, inserter(va, annotation), gs) }
    copy(rdd = newRDD, vaSignature = newSignature)
  }

  def annotateSamples(annotations: Map[String, Annotation], signature: Type,
    path: List[String]): VariantSampleMatrix[T] = {

    val (newSignature, inserter) = insertSA(signature, path)

    val newAnnotations = sampleIds.zipWithIndex.map { case (id, i) =>
      val sa = sampleAnnotations(i)
      inserter(sa, annotations.get(id))
    }

    copy(sampleAnnotations = newAnnotations, saSignature = newSignature)
  }

  def queryVA(code: String): (BaseType, Querier) = {

    val st = Map(Annotation.VARIANT_HEAD -> (0, vaSignature))
    val ec = EvalContext(st)
    val a = ec.a

    val (t, f) = Parser.parse(code, ec)

    val f2: Annotation => Option[Any] = { annotation =>
      a(0) = annotation
      f()
    }

    (t, f2)
  }

  def querySA(code: String): (BaseType, Querier) = {

    val st = Map(Annotation.SAMPLE_HEAD -> (0, saSignature))
    val ec = EvalContext(st)
    val a = ec.a

    val (t, f) = Parser.parse(code, ec)

    val f2: Annotation => Option[Any] = { annotation =>
      a(0) = annotation
      f()
    }

    (t, f2)
  }

  def queryGlobal(path: String): (BaseType, Option[Annotation]) = {
    val st = Map(Annotation.GLOBAL_HEAD -> (0, globalSignature))
    val ec = EvalContext(st)
    val a = ec.a

    val (t, f) = Parser.parse(path, ec)

    val f2: Annotation => Option[Any] = { annotation =>
      a(0) = annotation
      f()
    }

    (t, f2(globalAnnotation))
  }

  def deleteVA(args: String*): (Type, Deleter) = deleteVA(args.toList)

  def deleteVA(path: List[String]): (Type, Deleter) = vaSignature.delete(path)

  def deleteSA(args: String*): (Type, Deleter) = deleteSA(args.toList)

  def deleteSA(path: List[String]): (Type, Deleter) = saSignature.delete(path)

  def deleteGlobal(args: String*): (Type, Deleter) = deleteGlobal(args.toList)

  def deleteGlobal(path: List[String]): (Type, Deleter) = globalSignature.delete(path)

  def insertVA(sig: Type, args: String*): (Type, Inserter) = insertVA(sig, args.toList)

  def insertVA(sig: Type, path: List[String]): (Type, Inserter) = {
    vaSignature.insert(sig, path)
  }

  def insertSA(sig: Type, args: String*): (Type, Inserter) = insertSA(sig, args.toList)

  def insertSA(sig: Type, path: List[String]): (Type, Inserter) = saSignature.insert(sig, path)

  def insertGlobal(sig: Type, args: String*): (Type, Inserter) = insertGlobal(sig, args.toList)

  def insertGlobal(sig: Type, path: List[String]): (Type, Inserter) = {
    globalSignature.insert(sig, path)
  }

  override def toString = s"VariantSampleMatrix(metadata=$metadata, rdd=$rdd, sampleIds=$sampleIds, nSamples=$nSamples, vaSignature=$vaSignature, saSignature=$saSignature, globalSignature=$globalSignature, sampleAnnotations=$sampleAnnotations, sampleIdsAndAnnotations=$sampleIdsAndAnnotations, globalAnnotation=$globalAnnotation, wasSplit=$wasSplit)"
}

// FIXME AnyVal Scala 2.11
class RichVDS(vds: VariantDataset) {
  def makeSchema(): StructType =
    StructType(Array(
      StructField("variant", Variant.schema, nullable = false),
      StructField("annotations", vds.vaSignature.schema, nullable = false),
      StructField("gs", GenotypeStream.schema, nullable = false)
    ))

  def makeSchemaForKudu(): StructType =
    makeSchema().add(StructField("sample_group", StringType, nullable = false))

  private def writeMetadata(sqlContext: SQLContext, dirname: String, compress: Boolean = true) = {
    if (!dirname.endsWith(".vds") && !dirname.endsWith(".vds/"))
      fatal(s"output path ending in `.vds' required, found `$dirname'")

    val hConf = vds.sparkContext.hadoopConfiguration
    hadoopMkdir(dirname, hConf)

    val sb = new StringBuilder
    writeTextFile(dirname + "/sa.schema", hConf) { out =>
      vds.saSignature.pretty(sb, 0, printAttrs = true)
      out.write(sb.result())
    }

    sb.clear()
    writeTextFile(dirname + "/va.schema", hConf) { out =>
      vds.vaSignature.pretty(sb, 0, printAttrs = true)
      out.write(sb.result())
    }

    sb.clear()
    writeTextFile(dirname + "/global.schema", hConf) { out =>
      vds.globalSignature.pretty(sb, 0, printAttrs = true)
      out.write(sb.result())
    }

    writeDataFile(dirname + "/metadata.ser", hConf) {
      dos => {
        val serializer = SparkEnv.get.serializer.newInstance()
        val ss = serializer.serializeStream(dos)
        ss
          .writeObject(VariantSampleMatrix.magicNumber)
          .writeObject(VariantSampleMatrix.fileVersion)
          .writeObject(vds.sampleIds)
          .writeObject(vds.sampleAnnotations)
          .writeObject(vds.globalAnnotation)
          .writeObject(vds.wasSplit)
        ss.close()
      }
    }
  }

  def write(sqlContext: SQLContext, dirname: String, compress: Boolean = true) {
    writeMetadata(sqlContext, dirname, compress)

    val vaSignature = vds.vaSignature
    val vaRequiresConversion = SparkAnnotationImpex.requiresConversion(vaSignature)

    val rowRDD = vds.rdd
      .map {
        case (v, va, gs) =>
          Row.fromSeq(Array(v.toRow,
            if (vaRequiresConversion) SparkAnnotationImpex.exportAnnotation(va, vaSignature) else va,
            gs.toGenotypeStream(v, compress).toRow))
      }
    sqlContext.createDataFrame(rowRDD, makeSchema())
      .write.parquet(dirname + "/rdd.parquet")
    // .saveAsParquetFile(dirname + "/rdd.parquet")
  }

  def writeKudu(sqlContext: SQLContext, dirname: String, tableName: String,
    master: String, vcfSeqDict: String, rowsPerPartition: Int,
    sampleGroup: String, compress: Boolean = true, drop: Boolean = false) {

    writeMetadata(sqlContext, dirname, compress)

    val vaSignature = vds.vaSignature

    val rowType = VariantSampleMatrix.kuduRowType(vaSignature)
    val rowRDD = vds.rdd
      .map { case (v, va, gs) =>
        KuduAnnotationImpex.exportAnnotation(Annotation(
          v.toRow,
          va,
          gs.toGenotypeStream(v, compress).toRow,
          sampleGroup), rowType).asInstanceOf[Row]
      }

    val schema: StructType = KuduAnnotationImpex.exportType(rowType).asInstanceOf[StructType]
    println(s"schema = $schema")
    val df = sqlContext.createDataFrame(rowRDD, schema)

    val kuduContext = new KuduContext(master)
    if (drop) {
      KuduUtils.dropTable(master, tableName)
      Thread.sleep(10 * 1000) // wait to avoid overwhelming Kudu service queue
    }
    if (!KuduUtils.tableExists(master, tableName)) {
      val hConf = sqlContext.sparkContext.hadoopConfiguration
      val headerLines = readFile(vcfSeqDict, hConf) { s =>
        Source.fromInputStream(s)
          .getLines()
          .takeWhile { line => line(0) == '#' }
          .toArray
      }
      val codec = new htsjdk.variant.vcf.VCFCodec()
      val seqDict = codec.readHeader(new BufferedLineIterator(headerLines.iterator.buffered))
        .getHeaderValue
        .asInstanceOf[htsjdk.variant.vcf.VCFHeader]
        .getSequenceDictionary

      val keys = Seq("variant__contig", "variant__start", "variant__ref",
        "variant__altAlleles_0__alt", "sample_group")
      kuduContext.createTable(tableName, schema, keys,
        KuduUtils.createTableOptions(schema, keys, seqDict, rowsPerPartition))
    }
    df.write
      .options(Map("kudu.master" -> master, "kudu.table" -> tableName))
      .mode("append").kudu

    println("Written to Kudu")
  }

  def eraseSplit: VariantDataset = {
    if (vds.wasSplit) {
      val (newSignatures1, f1) = vds.deleteVA("wasSplit")
      val vds1 = vds.copy(vaSignature = newSignatures1)
      val (newSignatures2, f2) = vds1.deleteVA("aIndex")
      vds1.copy(wasSplit = false,
        vaSignature = newSignatures2,
        rdd = vds1.rdd.map {
          case (v, va, gs) =>
            (v, f2(f1(va)), gs.lazyMap(g => g.copy(fakeRef = false)))
        })
    } else
      vds
  }

  def withGenotypeStream(compress: Boolean = false): VariantDataset =
    vds.copy(rdd = vds.rdd.map { case (v, va, gs) =>
      (v, va, gs.toGenotypeStream(v, compress = compress))
    })
}
