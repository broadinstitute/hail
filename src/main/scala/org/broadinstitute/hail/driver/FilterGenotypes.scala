package org.broadinstitute.hail.driver

import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.expr._
import org.broadinstitute.hail.methods._
import org.broadinstitute.hail.annotations._
import org.broadinstitute.hail.variant._
import org.kohsuke.args4j.{Option => Args4jOption}
import scala.collection.mutable.ArrayBuffer

object FilterGenotypes extends Command {

  class Options extends BaseOptions {
    @Args4jOption(required = false, name = "--keep", usage = "Keep only listed samples in current dataset")
    var keep: Boolean = false

    @Args4jOption(required = false, name = "--remove", usage = "Remove listed samples from current dataset")
    var remove: Boolean = false

    @Args4jOption(required = true, name = "-c", aliases = Array("--condition"),
      usage = "Filter condition expression")
    var condition: String = _
  }

  def newOptions = new Options

  def name = "filtergenotypes"

  def description = "Filter genotypes in current dataset"

  def run(state: State, options: Options): State = {
    val sc = state.sc
    val vds = state.vds
    val vas = vds.vaSignature
    val sas = vds.saSignature

    if (!options.keep && !options.remove)
      fatal(name + ": one of `--keep' or `--remove' required")

    val keep = options.keep

    val symTab = Map(
      "v" ->(0, TVariant),
      "va" ->(1, vas),
      "s" ->(2, TSample),
      "sa" ->(3, sas),
      "g" ->(4, TGenotype))
    val a = new ArrayBuffer[Any]()
    for (_ <- symTab)
      a += null
    val f: () => Option[Boolean] = Parser.parse[Boolean](options.condition, symTab, a, TBoolean)

    val noCall = Genotype()
    val newVDS = vds.mapValuesWithAll(
      (v: Variant, va: Annotation, s: String, sa: Annotation, g: Genotype) => {
        a(0) = v
        a(1) = va
        a(2) = s
        a(3) = sa
        a(4) = g
        if (Filter.keepThis(f(), keep))
          g
        else
          noCall
      })
    state.copy(vds = newVDS)
  }
}
