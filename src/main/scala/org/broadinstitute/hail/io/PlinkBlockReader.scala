package org.broadinstitute.hail.io

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.{RecordReader, FileSplit}

class PlinkBlockReader(job: Configuration, split: FileSplit) extends IndexedBinaryBlockReader(job, split) {

    override def getIndices(start: Long, end: Long): Array[BlockIndex] = {
      val nSamples = job.getInt("nSamples", 0)
      val blockLength = ((nSamples / 4.00) + .75).toInt
      // the -3 is for magic numbers at the beginning of the bed file
//      println(s"start=$start, end=$end")
      val firstBlock = (start - 3) / blockLength
      val lastBlock = (end - 4) / blockLength
//      println(s"first=$firstBlock, last=$lastBlock")
      val range = (firstBlock to lastBlock).toArray
//      println("firstRange=%d, lastRange=%d".format(range(0), range.last))
      (firstBlock until lastBlock).map(ind => BlockIndex(3 + ind*blockLength, blockLength, ind.toInt)).toArray
    }
}
