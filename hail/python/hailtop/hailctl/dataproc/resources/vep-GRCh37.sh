#!/bin/bash

export ASSEMBLY=GRCh37
export VEP_DOCKER_IMAGE=konradjk/vep85_loftee:1.0.3

mkdir -p /vep_data/loftee_data
mkdir -p /vep_data/homo_sapiens

# Install docker
apt-get update
apt-get -y install \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg2 \
    software-properties-common \
    tabix
curl -fsSL https://download.docker.com/linux/debian/gpg | sudo apt-key add -
sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/debian $(lsb_release -cs) stable"
apt-get update
apt-get install -y --allow-unauthenticated docker-ce

# Get VEP cache and LOFTEE data
gsutil cp gs://hail-common/vep/vep/vep85-loftee-gcloud.json /vep_data/vep85-gcloud.json
ln -s /vep_data/vep85-gcloud.json /vep_data/vep-gcloud.json

gsutil -m cp -r gs://hail-common/vep/vep/loftee-beta/${ASSEMBLY}/* /vep_data/loftee_data/ &
gsutil -m cp -r gs://hail-common/vep/vep/Plugins /vep_data &
gsutil -m cp -r gs://hail-common/vep/vep/homo_sapiens/85_${ASSEMBLY} /vep_data/homo_sapiens/ &
docker pull ${VEP_DOCKER_IMAGE} &
wait

# New version
gsutil -m cp gs://hail-us-vep/loftee-beta/${ASSEMBLY}.tar /vep_data2/${ASSEMBLY}.tar &
gsutil -m cp gs://hail-us-vep/Plugins.tar /vep_data2/Plugins.tar &
gsutil -m cp gs://hail-us-vep/homo-sapiens/85_${ASSEMBLY}.tar /vep_data2/homo_sapiens/85_${ASSEMBLY}.tar &
docker pull ${VEP_DOCKER_IMAGE} &
wait

tar -xvf /vep_data2/${ASSEMBLY}.tar
rm /vep_data2/${ASSEMBLY}.tar
tar -xvf /vep_data2/Plugins.tar
rm /vep_data2/Plugins.tar
tar -xvf /vep_data2/homo_sapiens/85_${ASSEMBLY}.tar
rm /vep_data2/homo_sapiens/85_${ASSEMBLY}.tar 


cat >/vep.c <<EOF
#include <unistd.h>
#include <stdio.h>

int
main(int argc, char *const argv[]) {
  if (setuid(geteuid()))
    perror( "setuid" );

  execv("/vep.sh", argv);
  return 0;
}
EOF
gcc -Wall -Werror -O2 /vep.c -o /vep
chmod u+s /vep

cat >/vep.sh <<EOF
#!/bin/bash

docker run -i -v /vep_data:/root/.vep:ro ${VEP_DOCKER_IMAGE} \
  perl /vep/ensembl-tools-release-85/scripts/variant_effect_predictor/variant_effect_predictor.pl \
  "\$@"
EOF
chmod +x /vep.sh

# Run VEP on the 1-variant VCF to create fasta.index file -- caution do not make fasta.index file writeable afterwards!
gsutil cp gs://hail-common/vep/vep/loftee-beta/GRCh37/1var.vcf file:///1var.vcf

cat /1var.vcf | docker run -i -v /vep_data:/root/.vep \
    ${VEP_DOCKER_IMAGE} \
    perl /vep/ensembl-tools-release-85/scripts/variant_effect_predictor/variant_effect_predictor.pl \
    --format vcf \
    --json \
    --everything \
    --allele_number \
    --no_stats \
    --cache --offline \
    --minimal \
    --assembly ${ASSEMBLY} \
    -o STDOUT
#    --plugin LoF,human_ancestor_fa:/root/.vep/loftee_data/human_ancestor.fa.gz,filter_position:0.05,min_intron_size:15,conservation_file:/root/.vep/loftee_data/phylocsf_gerp.sql,gerp_file:/root/.vep/loftee_data/GERP_scores.final.sorted.txt.gz
