package org.broadinstitute.hail.driver

import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.annotations._
import org.broadinstitute.hail.expr
import org.broadinstitute.hail.expr._
import org.broadinstitute.hail.methods.Aggregators
import org.kohsuke.args4j.{Option => Args4jOption}

import scala.collection.mutable

object AnnotateGlobalExpr extends Command {

  class Options extends BaseOptions {
    @Args4jOption(required = true, name = "-c", aliases = Array("--condition"),
      usage = "Annotation expression")
    var condition: String = _
  }

  def newOptions = new Options

  def name = "annotateglobal expr"

  def description = "Annotate global table"

  def supportsMultiallelic = true

  def requiresVDS = true

  def run(state: State, options: Options): State = {
    val vds = state.vds

    val cond = options.condition

    val aggECV = EvalContext(Map(
      "v" -> (0, TVariant),
      "va" -> (1, vds.vaSignature),
      "global" -> (2, vds.globalSignature)))
    val aggECS = EvalContext(Map(
      "s" -> (0, TSample),
      "sa" -> (1, vds.saSignature),
      "global" -> (2, vds.globalSignature)))
    val symTab = Map(
      "global" -> (0, vds.globalSignature),
      "variants" -> (-1, TAggregable(aggECV)),
      "samples" -> (-1, TAggregable(aggECS)))


    val ec = EvalContext(symTab)
    aggECS.set(2, vds.globalAnnotation)
    aggECV.set(2, vds.globalAnnotation)

    val (parseTypes, fns) = Parser.parseAnnotationArgs(cond, ec, Annotation.GLOBAL_HEAD)

    val inserterBuilder = mutable.ArrayBuilder.make[Inserter]

    val finalType = parseTypes.foldLeft(vds.globalSignature) { case (v, (ids, signature)) =>
      val (s, i) = v.insert(signature, ids)
      inserterBuilder += i
      s
    }

    val inserters = inserterBuilder.result()

    if (aggECV.aggregationFunctions.nonEmpty) {
      val (zVal, seqOp, combOp, resOp) = Aggregators.makeFunctions(aggECV)

      val result = vds.variantsAndAnnotations
        .treeAggregate(zVal)(seqOp, combOp)
      resOp(result)
    }

    if (aggECS.aggregationFunctions.nonEmpty) {

      val (zVal, seqOp, combOp, resOp) = Aggregators.makeFunctions(aggECS)

      val result = vds.sampleIdsAndAnnotations
        .aggregate(zVal)(seqOp, combOp)
      resOp(result)
    }

    ec.set(0, vds.globalAnnotation)

    val ga = inserters
      .zip(fns.map(_ ()))
      .foldLeft(vds.globalAnnotation) { case (a, (ins, res)) =>
        ins(a, res)
      }

    state.copy(vds = vds.copy(
      globalAnnotation = ga,
      globalSignature = finalType)
    )
  }
}

