package org.broadinstitute.hail.driver

import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.annotations.{Annotation, Inserter}
import org.broadinstitute.hail.expr._
import org.broadinstitute.hail.methods.Aggregators
import org.kohsuke.args4j.{Option => Args4jOption}

import scala.collection.mutable

object AnnotateSamplesExpr extends Command {

  class Options extends BaseOptions {
    @Args4jOption(required = true, name = "-c", aliases = Array("--condition"),
      usage = "Annotation condition")
    var condition: String = _
  }

  def newOptions = new Options

  def name = "annotatesamples expr"

  def description = "Annotate samples programatically"

  def supportsMultiallelic = true

  def requiresVDS = true

  def run(state: State, options: Options): State = {
    val vds = state.vds

    val cond = options.condition
    val aggregationEC = EvalContext(Map(
      "v" -> (0, TVariant),
      "va" -> (1, vds.vaSignature),
      "s" -> (2, TSample),
      "sa" -> (3, vds.saSignature),
      "g" -> (4, TGenotype),
      "global" -> (5, vds.globalSignature)))

    val symTab = Map(
      "s" -> (0, TSample),
      "sa" -> (1, vds.saSignature),
      "global" -> (2, vds.globalSignature),
      "gs" -> (-1, TAggregable(aggregationEC)))

    val ec = EvalContext(symTab)
    ec.set(2, vds.globalAnnotation)
    aggregationEC.set(5, vds.globalAnnotation)

    val (parseTypes, fns) = Parser.parseAnnotationArgs(cond, ec, Annotation.SAMPLE_HEAD)

    val inserterBuilder = mutable.ArrayBuilder.make[Inserter]
    val finalType = parseTypes.foldLeft(vds.saSignature) { case (sas, (ids, signature)) =>

      val (s, i) = sas.insert(signature, ids)
      inserterBuilder += i
      s
    }
    val inserters = inserterBuilder.result()

    val aggregatorA = aggregationEC.a

    val sampleAggregationOption = Aggregators.buildSampleAggregations(vds, aggregationEC)

    val newAnnotations = vds.sampleIdsAndAnnotations.map { case (s, sa) =>

      ec.setAll(s, sa)

      sampleAggregationOption.foreach(f => f.apply(s))

      fns.zip(inserters)
        .foldLeft(sa) { case (sa, (fn, inserter)) =>
          inserter(sa, fn())
        }
    }
    state.copy(vds = vds.copy(
      sampleAnnotations = newAnnotations,
      saSignature = finalType
    ))
  }
}
