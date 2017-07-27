package is.hail.io.vcf

import com.intel.genomicsdb.GenomicsDBFeatureReader
import htsjdk.variant.variantcontext.VariantContext
import htsjdk.variant.vcf.VCFHeader
import is.hail.HailContext
import is.hail.annotations.Annotation
import is.hail.expr.{TStruct, _}
import is.hail.utils._
import is.hail.variant.{VSMLocalValue, VSMMetadata, VariantSampleMatrix}
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConversions._
import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.reflect.ClassTag

object LoadGDB {

  def apply[T](hc: HailContext,
               reader: HtsjdkRecordReader[T],
               loaderJSONFile: String,
               queryJSONFile: String,
               tiledbWorkspace: String,
               arrayName: String,
               referenceGenome: String,
               nPartitions: Option[Int] = None,
               dropSamples: Boolean = false)(implicit tct: ClassTag[T]): VariantSampleMatrix[T] = {
    val sc = hc.sc

    val codec = new htsjdk.variant.vcf.VCFCodec()

    val gdbReader = new GenomicsDBFeatureReader(loaderJSONFile, tiledbWorkspace, arrayName, referenceGenome, codec)

    gdbReader.initialize(loaderJSONFile, queryJSONFile, codec) //query file contains path to VCF header

    val header = gdbReader
      .getHeader
      .asInstanceOf[VCFHeader]

    // FIXME apply descriptions when HTSJDK is fixed to expose filter descriptions
    var filters: Map[String, String] = header
      .getFilterLines
      .toList
      //(ID, description)
      .map(line => (line.getID, ""))
      .toMap

    if (filters.size > 1 && filters.contains("PASS")) //remove extra PASS filter if there are others
      filters = filters.tail //FIXME: remove operation on Maps doesn't work -- what is a better way to remove PASS?

    val infoHeader = header.getInfoHeaderLines
    val infoSignature = LoadVCF.headerSignature(infoHeader)

    val formatHeader = header.getFormatHeaderLines
    val genotypeSignature: Type =
      if (reader.genericGenotypes) {
        val callFields = reader.asInstanceOf[GenericRecordReader].callFields
        LoadVCF.headerSignature(formatHeader, genericGenotypes = true, callFields).getOrElse(TStruct.empty)
      } else TGenotype

    val variantAnnotationSignatures = TStruct(
      Array(
        Some(Field("rsid", TString, 0)),
        Some(Field("qual", TDouble, 1)),
        Some(Field("filters", TSet(TString), 2, filters)),
        infoSignature.map(sig => Field("info", sig, 3))
      ).flatten)

    val sampleIds: Array[String] =
      if (dropSamples)
        Array.empty
      else gdbReader
        .query("", 0, 1)
        .asScala
        .next
        .getSampleNamesOrderedByName
        .toArray(new Array[String](0))

    val records = gdbReader
      .iterator
      .asScala
      .map(vc => reader.readRecord(vc, infoSignature, genotypeSignature))
      .toSeq

    val recordRDD = sc.parallelize(records, nPartitions.getOrElse(sc.defaultMinPartitions))

    val justVariants = recordRDD.map(_._1)
    justVariants.persist(StorageLevel.MEMORY_AND_DISK)

    val noMulti = justVariants.forall(_.nAlleles == 2)

    if (noMulti)
      info("No multiallelics detected.")
    else
      info("Multiallelic variants detected. Some methods require splitting or filtering multiallelics first.")

    val rdd = recordRDD.toOrderedRDD(justVariants)

    justVariants.unpersist()

    new VariantSampleMatrix[T](hc, VSMMetadata(
      TString,
      TStruct.empty,
      TVariant,
      variantAnnotationSignatures,
      TStruct.empty,
      genotypeSignature,
      isGenericGenotype = reader.genericGenotypes,
      wasSplit = noMulti),
      VSMLocalValue(Annotation.empty,
        sampleIds,
        Annotation.emptyIndexedSeq(sampleIds.length)),
      rdd)
  }
}
