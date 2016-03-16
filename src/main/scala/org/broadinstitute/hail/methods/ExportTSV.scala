package org.broadinstitute.hail.methods

import org.apache.hadoop
import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.annotations._
import org.broadinstitute.hail.expr
import org.broadinstitute.hail.variant.{VariantMetadata, Sample, Variant, Genotype}
import scala.io.Source
import scala.language.implicitConversions

object ExportTSV {

  def parseColumnsFile(
    symTab: Map[String, (Int, expr.Type)],
    a: Array[Any],
    path: String,
    hConf: hadoop.conf.Configuration): (Option[String], Array[() => Any]) = {
    val pairs = readFile(path, hConf) { reader =>
      Source.fromInputStream(reader)
        .getLines()
        .filter(!_.isEmpty)
        .map { line =>
          val cols = line.split("\t")
          if (cols.length != 2)
            fatal("invalid .columns file.  Include 2 columns, separated by a tab")
          (cols(0), cols(1))
        }.toArray
    }

    val header = pairs.map(_._1).mkString("\t")
    val fs = pairs.map { case (_, e) =>
      expr.Parser.parse[Any](symTab, a, e)
    }

    (Some(header), fs)
  }
}
