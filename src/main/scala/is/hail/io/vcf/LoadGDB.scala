package is.hail.io.vcf

import com.intel.genomicsdb.GenomicsDBFeatureReader
import htsjdk.variant.vcf.VCFHeader
import is.hail.HailContext
import is.hail.annotations.Annotation
import is.hail.expr.{TStruct, _}
import is.hail.utils._
import is.hail.variant.{VSMLocalValue, VSMMetadata, VariantSampleMatrix, Locus, Variant}
import org.apache.spark.storage.StorageLevel
import org.json4s._

import scala.collection.JavaConversions._
import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.reflect.ClassTag
import java.io.File


case class QueryJSON(workspace: String,
                     array: List[String],
                     query_column_ranges: List[List[Int]],
                     query_attributes: List[String] = List(),
                     query_row_ranges: Option[List[List[Int]]] = None,
                     vid_mapping_file: Option[String],
                     callset_mapping_file: Option[String],
                     reference_genome: String,
                     vcf_header_filename: Option[String],
                     max_diploid_alt_alleles_that_can_be_genotyped: Int = 50,
                     vcf_output_filename: Option[List[String]] = None,
                     vcf_output_format: Option[String] = None,
                     produce_GT_field: Boolean = true,
                     index_output_VCF: Boolean = false,
                     combined_vcf_records_buffer_size_limit: Int = 1048576)

object LoadGDB {

  /*
  def createQueryJSON(): String = {
    val tempFile = File.createTempFile("sample2query", ".json")
    val tempFilePath = tempFile.getCanonicalFile
    jackson.Serialization.writePretty(QueryJSON, java.io.Writer)
  }*/

  def apply[T >: Null](hc: HailContext,
               reader: HtsjdkRecordReader[T],
               loaderJSONFile: String,
               queryJSONFile: String,
               tiledbWorkspace: String,
               arrayName: String,
               referenceGenome: String,
               nPartitions: Option[Int] = None,
               dropSamples: Boolean = false)(implicit tct: ClassTag[T]): VariantSampleMatrix[Locus, Variant, T] = {
    val sc = hc.sc

    val codec = new htsjdk.variant.vcf.VCFCodec()

    val gdbReader = new GenomicsDBFeatureReader(loaderJSONFile, queryJSONFile, codec)

    val header = gdbReader
      .getHeader
      .asInstanceOf[VCFHeader]

    // FIXME apply descriptions when HTSJDK is fixed to expose filter descriptions
    val immutableFilters: Map[String, String] = header
      .getFilterLines
      .toList
      //(ID, description)
      .map(line => (line.getID, ""))
      .toMap

    var filters = immutableFilters

    if (immutableFilters.size > 1 && immutableFilters.contains("PASS")) { //remove extra PASS filter if there are others
      val mutableFilters = collection.mutable.Map(immutableFilters.toSeq: _*)
      mutableFilters.remove("PASS")
      filters = mutableFilters.toMap
    }

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
      else {
        val it = gdbReader.query("", 0, 1).asScala
        if (it.hasNext)
          it.next.getSampleNamesOrderedByName.toArray(new Array[String](0))
        else
          Array.empty
      }

    val records = gdbReader
      .iterator
      .asScala
      .map(vc => {
        reader.readRecord(vc, infoSignature, genotypeSignature)
      })
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

    new VariantSampleMatrix[Locus, Variant, T](hc, VSMMetadata(
      TString,
      TStruct.empty,
      TVariant,
      variantAnnotationSignatures,
      TStruct.empty,
      genotypeSignature,
      wasSplit = noMulti),
      VSMLocalValue(Annotation.empty,
        sampleIds,
        Annotation.emptyIndexedSeq(sampleIds.length)),
      rdd)
  }
}
