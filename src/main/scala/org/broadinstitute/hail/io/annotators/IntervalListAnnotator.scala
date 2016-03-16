package org.broadinstitute.hail.io.annotators

import org.apache.hadoop
import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.annotations.{Signature, Annotation, SimpleSignature}
import org.broadinstitute.hail.expr
import org.broadinstitute.hail.variant.{Interval, IntervalList, Variant}

object IntervalListAnnotator {
  def apply(filename: String, hConf: hadoop.conf.Configuration): (IntervalList, Signature) = {
    // this annotator reads files in the UCSC BED spec defined here: https://genome.ucsc.edu/FAQ/FAQformat.html#format1

    readLines(filename, hConf) { lines =>

      fatalIf(lines.isEmpty, "empty interval file")

      val firstLine = lines.next()
      val (getString, signature) = firstLine.value match {
        case IntervalList.intervalRegex(contig, start_str, end_str) => (false, SimpleSignature(expr.TBoolean))
        case line if line.split("""\s+""").length == 5 => (true, SimpleSignature(expr.TString))
        case _ => fatal("unsupported interval list format")
      }

      val iList = IntervalList((Iterator(firstLine) ++ lines)
        .map { l => l.transform(line => {
          if (getString) {
            val Array(contig, start, end, direction, target) = line.value.split("""\s+""")
            Interval(contig, start.toInt, end.toInt, Some(target))
          }
          else {
            line.value match {
              case IntervalList.intervalRegex(contig, start_str, end_str) =>
                Interval(contig, start_str.toInt, end_str.toInt, Some(true))
              case _ => fatal("Inconsistent interval file")
            }
          }
        })
        }
        .toTraversable)

      (iList, signature)
    }
  }
}
