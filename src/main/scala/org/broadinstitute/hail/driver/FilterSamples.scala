package org.broadinstitute.hail.driver

import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.expr._
import org.broadinstitute.hail.methods._
import org.broadinstitute.hail.annotations._
import org.kohsuke.args4j.{Option => Args4jOption}
import scala.collection.mutable.ArrayBuffer

import scala.io.Source

object FilterSamples extends Command {

  class Options extends BaseOptions {
    @Args4jOption(required = false, name = "--all", usage = "Filter all samples")
    var all: Boolean = false

    @Args4jOption(required = false, name = "-c", aliases = Array("--condition"),
      usage = "Filter condition: expression or .sample_list file (one sample name per line)")
    var condition: String = _

    @Args4jOption(required = false, name = "--keep", usage = "Keep only listed samples in current dataset")
    var keep: Boolean = false

    @Args4jOption(required = false, name = "--remove", usage = "Remove listed samples from current dataset")
    var remove: Boolean = false

  }

  def newOptions = new Options

  def name = "filtersamples"

  def description = "Filter samples in current dataset"

  override def supportsMultiallelic = true

  def run(state: State, options: Options): State = {
    val vds = state.vds

    if ((options.keep && options.remove)
      || (!options.keep && !options.remove))
      fatal("one `--keep' or `--remove' required, but not both")

    if ((options.all && options.condition != null)
      || (!options.all && options.condition == null))
      fatal("one `--all' or `-c' required, but not both")

    if (options.all) {
      if (options.keep)
        return state
      else
        return state.copy(
          vds = state.vds.dropSamples())
    }

    val keep = options.keep
    val sas = vds.saSignature
    val p = options.condition match {
      case f if f.endsWith(".sample_list") =>
        val indexOfSample: Map[String, Int] = vds.sampleIds.zipWithIndex.toMap
        val samples = readFile(f, state.hadoopConf) { reader =>
          Source.fromInputStream(reader)
            .getLines()
            .filter(line => !line.isEmpty)
            .toSet
        }
        (s: String, sa: Annotation) => Filter.keepThis(samples.contains(s), keep)
      case c: String =>
        val symTab = Map(
          "s" -> (0, TSample),
          "sa" -> (1, sas))
        val a = new ArrayBuffer[Any]()
        for (_ <- symTab)
          a += null
        val f: () => Option[Boolean] = Parser.parse[Boolean](c, symTab, a, TBoolean)
        (s: String, sa: Annotation) => {
          a(0) = s
          a(1) = sa
          Filter.keepThis(f(), keep)
        }
    }

    state.copy(vds = vds.filterSamples(p))
  }
}
