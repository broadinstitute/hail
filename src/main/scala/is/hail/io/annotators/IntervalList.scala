package is.hail.io.annotators

import is.hail.HailContext
import is.hail.annotations.Annotation
import is.hail.expr.types._
import is.hail.table.Table
import is.hail.utils.{Interval, _}
import is.hail.variant._
import org.apache.spark.sql.Row


object IntervalList {

  val intervalRegex = """([^:]*)[:\t](\d+)[\-\t](\d+)""".r

  def read(hc: HailContext, filename: String, rg: Option[ReferenceGenome] = Some(ReferenceGenome.defaultReference)): Table = {
    val hasValue = hc.hadoopConf.readLines(filename) {
      lines =>
        val skipHeader = lines.filter(l => !l.value.isEmpty && l.value(0) != '@')

        if (skipHeader.isEmpty)
          fatal("empty interval file")

        val firstLine = skipHeader.next()
        firstLine.map {
          case intervalRegex(contig, start_str, end_str) => false
          case line if line.split("""\s+""").length == 5 => true
          case _ => fatal(
            """invalid interval format.  Acceptable formats:
              |  `chr:start-end'
              |  `chr  start  end' (tab-separated)
              |  `chr  start  end  strand  target' (tab-separated, strand is `+' or `-')""".stripMargin)
        }.value
    }

    val locusSchema = TLocus.schemaFromRG(rg)

    val schema = if (hasValue)
      TStruct("interval" -> TInterval(locusSchema), "target" -> TString())
    else
      TStruct("interval" -> TInterval(locusSchema))

    implicit val ord = locusSchema.ordering

    val rdd = hc.sc.textFileLines(filename)
      .filter(l => !l.value.isEmpty && l.value(0) != '@')
      .map {
        _.map { line =>
          if (hasValue) {
            val split = line.split("\\s+")
            split match {
              case Array(contig, start, end, dir, target) =>
                val interval = Interval(Locus.annotationFromRG(contig, start.toInt, rg), Locus.annotationFromRG(contig, end.toInt, rg),
                  includeStart = true, includeEnd = true)
                Row(interval, target)
              case arr => fatal(s"expected 5 fields, but found ${ arr.length }")
            }
          } else {
            line match {
              case intervalRegex(contig, start, end) =>
                val interval = Interval(Locus.annotationFromRG(contig, start.toInt, rg), Locus.annotationFromRG(contig, end.toInt, rg),
                  includeStart = true, includeEnd = true)
                Row(interval)
              case _ => fatal("invalid interval")
            }
          }
        }.value
      }

    Table(hc, rdd, schema, Array("interval"))
  }
}
