<div class="cmdhead"></div>

<div class="description"></div>

<div class="synopsis"></div>

<div class="options"></div>

<div class="cmdsubsection">
### Implementation Details:
The `vep` command runs the [Variant Effect Predictor](http://www.ensembl.org/info/docs/tools/vep/index.html) on the current dataset and adds the result as a variant annotation at `va.vep` by default.  `vep` runs VEP with the [LOFTEE](https://github.com/konradjk/loftee) plugin.

If the root already exists and its schema matches the VEP schema, then
`vep` only runs VEP for variants for which the value of the root is
missing.

The `vep` command needs a configuration file to tell it how it run
VEP. The format is a [.properties](https://en.wikipedia.org/wiki/.properties) file.
Roughly, each line defines a property as a key-value pair of the form `key = value`. `vep` supports the following properties:

 - `hail.vep.perl`: Location of Perl.  Optional, default: perl.
 - `hail.vep.perl5lib`: Value for the PERL5LIB environment variable when invoking VEP.  Optional, by default PERL5LIB is not set.
 - `hail.vep.path`: Value of the PATH environment variable when invoking VEP.  Optional, by default PATH is not set.
 - `hail.vep.location`: Location of the VEP Perl script.  Required.
 - `hail.vep.cache_dir`: Location of the VEP cache dir, passed to VEP with the `--dir` option.  Required.
 - `hail.vep.lof.human_ancestor`: Location of the human ancestor file for the Loftee plugin.  Required.
 - `hail.vep.lof.conservation_file`: Location of the conservation file for the Loftee plugin.  Required.

VEP is invoked as follows:
```
<hail.vep.perl>
  <hail.vep.location>
  --format vcf
  --json
  --everything
  --allele_number
  --no_stats
  --cache --offline
  --dir <hail.vep.cache_dir>
  --fasta <hail.vep.cache_dir>/homo_sapiens/81_GRCh37/Homo_sapiens.GRCh37.75.dna.primary_assembly.fa
  --minimal
  --assembly GRCh37
  --plugin LoF,human_ancestor_fa:$<hail.vep.lof.human_ancestor>,filter_position:0.05,min_intron_size:15,conservation_file:<hail.vep.lof.conservation_file>
  -o STDOUT
```
</div>

<div class="cmdsubsection">
### Examples:

```
$ hail importvcf /path/to/file.vcf vep --config /path/to/vep.properties write -o /path/to/file.vds
```

Here is an example `vep.properties` configuration file:

```
hail.vep.perl = /usr/bin/perl
hail.vep.path = /usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin
hail.vep.location = /path/to/vep/ensembl-tools-release-81/scripts/variant_effect_predictor/variant_effect_predictor.pl
hail.vep.cache_dir = /path/to/vep
hail.vep.lof.human_ancestor = /path/to/loftee_data/human_ancestor.fa.gz
hail.vep.lof.conservation_file = /path/to/loftee_data//phylocsf.sql
```
</div>

<div class="cmdsubsection">
### <a name="vep_annotations"></a> Annotations:

The VEP output has the following schema, which can of course be viewed
with the [`printschema`](#printschema) command after annotating with VEP.

```
Struct{
  assembly_name: String,
  allele_string: String,
  colocated_variants: Array[Struct{
    aa_allele: String,
    aa_maf: Double,
    afr_allele: String,
    afr_maf: Double,
    allele_string: String,
    amr_allele: String,
    amr_maf: Double,
    clin_sig: Array[String],
    end: Int,
    eas_allele: String,
    eas_maf: Double,
    ea_allele: String,,
    ea_maf: Double,
    eur_allele: String,
    eur_maf: Double,
    exac_adj_allele: String,
    exac_adj_maf: Double,
    exac_allele: String,
    exac_afr_allele: String,
    exac_afr_maf: Double,
    exac_amr_allele: String,
    exac_amr_maf: Double,
    exac_eas_allele: String,
    exac_eas_maf: Double,
    exac_fin_allele: String,
    exac_fin_maf: Double,
    exac_maf: Double,
    exac_nfe_allele: String,
    exac_nfe_maf: Double,
    exac_oth_allele: String,
    exac_oth_maf: Double,
    exac_sas_allele: String,
    exac_sas_maf: Double,
    id: String,
    minor_allele: String,
    minor_allele_freq: Double,
    phenotype_or_disease: Int,
    pubmed: Array[Int],
    sas_allele: String,
    sas_maf: Double,
    somatic: Int,
    start: Int,
    strand: Int
  }],
  end: Int,
  id: String,
  input: String,
  intergenic_consequences: Array[Struct{
    allele_num: Int,
    consequence_terms: Array[String],
    impact: String,
    minimised: Int,
    variant_allele: String
  }],
  most_severe_consequence: String,
  motif_feature_consequences: Array[Struct{
    allele_num: Int,
    consequence_terms: Array[String],
    high_inf_pos: String,
    impact: String,
    minimised: Int,
    motif_feature_id: String,
    motif_name: String,
    motif_pos: Int,
    motif_score_change: Double,
    strand: Int,
    variant_allele: String
  }],
  regulatory_feature_consequences: Array[Struct{
    allele_num: Int,
    biotype: String,
    consequence_terms: Array[String],
    impact: String,
    minimised: Int,
    regulatory_feature_id: String,
    variant_allele: String
  }],
  seq_region_name: String,
  start: Int,
  strand: Int,
  transcript_consequences: Array[Struct{
    allele_num: Int,
    amino_acids: String,
    biotype: String,
    canonical: Int,
    ccds: String,
    cdna_start: Int,
    cdna_end: Int,
    cds_end: Int,
    cds_start: Int,
    codons: String,
    consequence_terms: Array[String],
    distance: Int,
    domains: Array[Struct{
      db: String
      name: String
    }],
    exon: String,
    gene_id: String,
    gene_pheno: Int,
    gene_symbol: String,
    gene_symbol_source: String,
    hgnc_id: Int,
    hgvsc: String,
    hgvsp: String,
    hgvs_offset: Int,
    impact: String,
    intron: String,
    lof: String,
    lof_flags: String,
    lof_filter: String,
    lof_info: String,
    minimised: Int,
    polyphen_prediction: String,
    polyphen_score: Double,
    protein_end: Int,
    protein_start: Int,
    protein_id: String,
    sift_prediction: String,
    sift_score: Double,
    strand: Int,
    swissprot: String,
    transcript_id: String,
    trembl: String,
    uniparc: String,
    variant_allele: String
  }],
  variant_class: String
}
```
</div>