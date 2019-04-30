#!/bin/bash

wget -O /tmp/baselineLD_v2.2_bedfiles.tgz https://data.broadinstitute.org/alkesgroup/LDSCORE/baselineLD_v2.2_bedfiles.tgz
mkdir /tmp/baselineLD_v2.2
tar -C /tmp/baselineLD_v2.2/ -xzvf /tmp/baselineLD_v2.2_bedfiles.tgz
for file in /tmp/baselineLD_v2.2/*.bed; do
    echo $file;
    bgzip -c $file | gsutil cp - gs://hail-datasets-raw-data/LDSC/baselineLD_v2.2/$(basename ${file}).bgz;
done
rm -r /tmp/baselineLD_v2.2*
