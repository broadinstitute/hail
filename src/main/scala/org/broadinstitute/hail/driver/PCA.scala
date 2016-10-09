package org.broadinstitute.hail.driver

import org.apache.spark.RangePartitioner
import org.apache.spark.storage.StorageLevel
import org.broadinstitute.hail.methods.SamplePCA
import org.kohsuke.args4j.{Option => Args4jOption}
import org.broadinstitute.hail.utils._

object PCA extends Command {
  def name = "pca"

  def description = "Run principle component analysis on the matrix of genotypes"

  class Options extends BaseOptions {
    @Args4jOption(required = true, name = "-o", aliases = Array("--output"), usage = "Output file for scores")
    var output: String = _

    @Args4jOption(required = false, name = "-k", aliases = Array("--components"), usage = "Number of principal components")
    var k: Int = 10

    @Args4jOption(required = false, name = "-l", aliases = Array("--loadings"), usage = "Output file for loadings")
    var lOutput: String = _

    @Args4jOption(required = false, name = "-e", aliases = Array("--eigenvalues"), usage = "Output file for eigenvalues")
    var eOutput: String = _

  }

  def newOptions = new Options

  def supportsMultiallelic = true

  def requiresVDS = true

  def run(state: State, options: Options): State = {

    val vds = state.vds

    val (scores, loadings, eigenvalues) = (new SamplePCA(options.k, options.lOutput != null, options.eOutput != null)) (vds)

    state.hadoopConf.writeTextFile(options.output) { s =>
      s.write("Sample\t" + (1 to options.k).map("PC" + _).mkString("\t") + "\n")
      for ((id, i) <- vds.sampleIds.zipWithIndex) {
        s.write(id)
        for (j <- 0 until options.k)
          s.write("\t" + scores(i, j))
        s.write("\n")
      }
    }

    loadings.foreach { l =>
      l.persist(StorageLevel.MEMORY_AND_DISK)
      val lSorted = l.repartitionAndSortWithinPartitions(new RangePartitioner(l.partitions.length, l))
      lSorted.persist(StorageLevel.MEMORY_AND_DISK)
      lSorted
        .map { case (v, vl) => v.contig + "\t" + v.start + "\t" + v.ref + "\t" + v.alt + "\t" + vl.mkString("\t") }
        .writeTable(options.lOutput, Some("chrom\tpos\tref\talt\t" + (1 to options.k).map("PC" + _).mkString("\t")))
      lSorted.unpersist()
      l.unpersist()
    }

    eigenvalues.foreach { es =>
      state.hadoopConf.writeTextFile(options.eOutput) { s =>
        for (e <- es) {
          s.write(e.toString)
          s.write("\n")
        }
      }
    }

    state
  }
}
