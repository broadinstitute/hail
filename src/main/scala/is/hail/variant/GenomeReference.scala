package is.hail.variant

import java.io.InputStream

import is.hail.check.Gen
import is.hail.expr.JSONExtractGenomeReference
import is.hail.utils._
import org.json4s._
import org.json4s.jackson.JsonMethods

import scala.collection.JavaConverters._
import scala.language.implicitConversions

abstract class GRBase extends Serializable {
  def isValidContig(contig: String): Boolean = ???

  def inX(contigIdx: Int): Boolean = ???

  def inX(contig: String): Boolean = ???

  def inY(contigIdx: Int): Boolean = ???

  def inY(contig: String): Boolean = ???

  def isMitochondrial(contigIdx: Int): Boolean = ???

  def isMitochondrial(contig: String): Boolean = ???

  def inXPar(locus: Locus): Boolean = ???

  def inYPar(locus: Locus): Boolean = ???

  def toJSON: JValue = ???

  def unify(concrete: GenomeReference): Boolean = ???

  def isBound: Boolean = ???

  def clear(): Unit = ???

  def subst(): GenomeReference = ???
}

case class GenomeReference(name: String, contigs: Array[Contig], xContigs: Set[String] = Set.empty[String],
  yContigs: Set[String] = Set.empty[String], mtContigs: Set[String] = Set.empty[String],
  par: Array[Interval[Locus]] = Array.empty[Interval[Locus]]) extends GRBase {

  require(contigs.length > 0, "Must have at least one contig in the genome reference.")

  require(xContigs.intersect(yContigs).isEmpty,
    s"Found the contigs `${ xContigs.intersect(yContigs).mkString(", ") }' in both xContigs and yContigs.")
  require(xContigs.intersect(mtContigs).isEmpty,
    s"Found the contigs `${ xContigs.intersect(mtContigs).mkString(", ") }' in both xContigs and mtContigs.")
  require(yContigs.intersect(mtContigs).isEmpty,
    s"Found the contigs `${ yContigs.intersect(mtContigs).mkString(", ") }' in both yContigs and mtContigs.")

  par.foreach { i =>
    require((xContigs.contains(i.start.contig) || yContigs.contains(i.start.contig)) &&
      (xContigs.contains(i.end.contig) || yContigs.contains(i.end.contig)),
      s"The contig name for PAR interval `$i' was not found in xContigs `$xContigs' or in yContigs `$yContigs'.")
  }

  val contigIndex: Map[String, Int] = contigs.map(_.name).zipWithIndex.toMap
  val contigNames: Set[String] = contigs.map(_.name).toSet

  val xNotInRef = xContigs.diff(contigNames)
  val yNotInRef = yContigs.diff(contigNames)
  val mtNotInRef = mtContigs.diff(contigNames)

  require(xNotInRef.isEmpty, s"The following X contig names were not found in the reference: `${ xNotInRef.mkString(", ") }'.")
  require(yNotInRef.isEmpty, s"The following Y contig names were not found in the reference: `${ yNotInRef.mkString(", ") }'.")
  require(mtNotInRef.isEmpty, s"The following MT contig names were not found in the reference: `${ mtNotInRef.mkString(", ") }'.")

  val xContigIndices = xContigs.map(contigIndex)
  val yContigIndices = yContigs.map(contigIndex)
  val mtContigIndices = mtContigs.map(contigIndex)

  override def isValidContig(contig: String): Boolean = contigNames.contains(contig)

  override def inX(contigIdx: Int): Boolean = xContigIndices.contains(contigIdx)

  override def inX(contig: String): Boolean = xContigs.contains(contig)

  override def inY(contigIdx: Int): Boolean = yContigIndices.contains(contigIdx)

  override def inY(contig: String): Boolean = yContigs.contains(contig)

  override def isMitochondrial(contigIdx: Int): Boolean = mtContigIndices.contains(contigIdx)

  override def isMitochondrial(contig: String): Boolean = mtContigs.contains(contig)

  override def inXPar(locus: Locus): Boolean = inX(locus.contig) && par.exists(_.contains(locus))

  override def inYPar(locus: Locus): Boolean = inY(locus.contig) && par.exists(_.contains(locus))

  override def toJSON: JValue = JObject(
    ("name", JString(name)),
    ("contigs", JArray(contigs.map(_.toJSON).toList)),
    ("xContigs", JArray(xContigs.map(JString(_)).toList)),
    ("yContigs", JArray(yContigs.map(JString(_)).toList)),
    ("mtContigs", JArray(mtContigs.map(JString(_)).toList)),
    ("par", JArray(par.map(_.toJSON(_.toJSON)).toList))
  )

  override def equals(other: Any): Boolean = {
    other match {
      case x: GenomeReference =>
        name == x.name &&
          contigs.sameElements(x.contigs) &&
          xContigs == x.xContigs &&
          yContigs == x.yContigs &&
          mtContigs == x.mtContigs &&
          par.sameElements(x.par)
      case _ => false
    }
  }

  override def unify(concrete: GenomeReference): Boolean = this == concrete

  override def isBound: Boolean = true

  override def clear() {}

  override def subst(): GenomeReference = this
}

object GenomeReference {
  var references: Map[String, GenomeReference] = Map("GRCh37" -> GRCh37, "GRCh38" -> GRCh38)

  def addReference(gr: GenomeReference) {
    references += (gr.name -> gr)
  }

  def getReference(name: String): GenomeReference = {
    references.get(name) match {
      case Some(gr) => gr
      case None => fatal(s"No genome reference with name `$name' exists. Names available: `${ references.keys.mkString(", ") }'.")
    }
  }

  def GRCh37: GenomeReference = fromResource("reference/grch37.json")

  def GRCh38: GenomeReference = fromResource("reference/grch38.json")

  def fromJSON(json: JValue): GenomeReference = json.extract[JSONExtractGenomeReference].toGenomeReference

  def fromResource(file: String): GenomeReference = loadFromResource[GenomeReference](file) {
    (is: InputStream) => fromJSON(JsonMethods.parse(is))
  }

  def gen: Gen[GenomeReference] = for {
    name <- Gen.identifier
    nContigs <- Gen.choose(3, 50)
    contigs <- Gen.distinctBuildableOfN[Array, Contig](nContigs, Contig.gen)
    contigNames = contigs.map(_.name).toSet
    xContig <- Gen.oneOfSeq(contigNames.toSeq)
    yContig <- Gen.oneOfSeq((contigNames - xContig).toSeq)
    mtContig <- Gen.oneOfSeq((contigNames - xContig - yContig).toSeq)
    parX <- Gen.distinctBuildableOfN[Array, Interval[Locus]](2, Interval.gen(Locus.gen(Seq(xContig))))
    parY <- Gen.distinctBuildableOfN[Array, Interval[Locus]](2, Interval.gen(Locus.gen(Seq(yContig))))
  } yield GenomeReference(name, contigs, Set(xContig), Set(yContig), Set(mtContig), parX ++ parY)

  def apply(name: java.lang.String, contigs: java.util.ArrayList[Contig],
    xContigs: java.util.ArrayList[String], yContigs: java.util.ArrayList[String],
    mtContigs: java.util.ArrayList[String], par: java.util.ArrayList[Interval[Locus]]): GenomeReference =
    GenomeReference(name, contigs.asScala.toArray, xContigs.asScala.toSet, yContigs.asScala.toSet, mtContigs.asScala.toSet,
      par.asScala.toArray)
}

case class GRVariable(var gr: GenomeReference = null) extends GRBase {

  override def toString = "GenomeReference"

  override def unify(concrete: GenomeReference): Boolean = {
    if (gr == null) {
      gr = concrete
      true
    } else
      gr == concrete
  }

  override def isBound: Boolean = gr != null

  override def clear() {
    gr = null
  }

  override def subst(): GenomeReference = {
    assert(gr != null)
    gr
  }
}

