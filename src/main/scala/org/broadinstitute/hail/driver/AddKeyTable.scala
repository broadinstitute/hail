package org.broadinstitute.hail.driver

import org.broadinstitute.hail.annotations.Annotation
import org.broadinstitute.hail.expr.{EvalContext, _}
import org.broadinstitute.hail.keytable.KeyTable
import org.broadinstitute.hail.methods.Aggregators
import org.broadinstitute.hail.utils._
import org.kohsuke.args4j.{Option => Args4jOption}

object AddKeyTable extends Command {

  class Options extends BaseOptions with TextTableOptions {
    @Args4jOption(required = true, name = "-k", aliases = Array("--key-cond"),
      usage = "Named key condition", metaVar = "EXPR")
    var keyCond: String = _

    @Args4jOption(required = true, name = "-a", aliases = Array("--agg-cond"),
      usage = "Named aggregation condition", metaVar = "EXPR")
    var aggCond: String = _

    @Args4jOption(required = true, name = "-n", aliases = Array("--name"),
      usage = "Name of new key table")
    var name: String = _

    @Args4jOption(required = false, name = "--key-id",
      usage = "ID of key in expressions")
    var keyId: String = "k"

    @Args4jOption(required = false, name = "--annotation-id",
      usage = "ID of key in expressions")
    var valueId: String = "ka"
  }

  def newOptions = new Options

  def name = "addkeytable"

  def description = "Creates a new key table with key(s) determined by named expressions and additional columns determined by named aggregator expressions"

  def supportsMultiallelic = true

  def requiresVDS = true

  override def hidden = true

  def run(state: State, options: Options): State = {

    val vds = state.vds
    val sc = state.sc

    val aggCond = options.aggCond
    val keyCond = options.keyCond

    val aggregationEC = EvalContext(Map(
      "v" -> (0, TVariant),
      "va" -> (1, vds.vaSignature),
      "s" -> (2, TSample),
      "sa" -> (3, vds.saSignature),
      "global" -> (4, vds.globalSignature)))

    val symTab = Map(
      "v" -> (0, TVariant),
      "va" -> (1, vds.vaSignature),
      "s" -> (2, TSample),
      "sa" -> (3, vds.saSignature),
      "global" -> (4, vds.globalSignature),
      "gs" -> (-1, BaseAggregable(aggregationEC, TGenotype)))

    val ec = EvalContext(symTab)
    val a = ec.a

    ec.set(4, vds.globalAnnotation)
    aggregationEC.set(4, vds.globalAnnotation)

    val (keyNames, keyParseTypes, keyF) = Parser.parseNamedArgs(keyCond, ec)
    val (aggNames, aggParseTypes, aggF) = Parser.parseNamedArgs(aggCond, ec)

    val keySignature = TStruct(keyNames.zip(keyParseTypes): _*)
    val aggSignature = TStruct(aggNames.zip(aggParseTypes): _*)

    if (keyNames.isEmpty)
      fatal("this module requires one or more named expr arguments as keys")

    if (aggNames.isEmpty) // FIXME: Make empty Annotation
      fatal("this module requires one or more named expr arguments to aggregate by key")

    val (zVals, _, combOp, resultOp) = Aggregators.makeFunctions(aggregationEC)
    val zvf = () => zVals.indices.map(zVals).toArray

    val seqOp = (array: Array[Aggregator], b: (Any, Any, Any, Any, Any)) => {
      val (v, va, s, sa, aggT) = b
      ec.set(0, v)
      ec.set(1, va)
      ec.set(2, s)
      ec.set(3, sa)
      for (i <- array.indices) {
        array(i).seqOp(aggT)
      }
      array
    }

    val kt = KeyTable(vds.mapPartitionsWithAll { it =>
      it.map { case (v, va, s, sa, g) =>
        ec.setAll(v, va, s, sa, g)
        val key = Annotation.fromSeq(keyF())
        (key, (v, va, s, sa, g))
      }
    }.aggregateByKey(zvf())(seqOp, combOp)
      .map { case (k, agg) =>
        resultOp(agg)
        (k, Annotation.fromSeq(aggF()))
      }, keySignature, aggSignature, options.keyId, options.valueId)

    state.copy(ktEnv = state.ktEnv + (options.name -> kt))
  }
}
