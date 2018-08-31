
import hail as hl

tissues = [
  'Adipose_Subcutaneous',
  'Adipose_Visceral_Omentum',
  'Adrenal_Gland',
  'Artery_Aorta',
  'Artery_Coronary', 
  'Artery_Tibial',
  'Brain_Amygdala', 
  'Brain_Anterior_cingulate_cortex_BA24',
  'Brain_Caudate_basal_ganglia',
  'Brain_Cerebellar_Hemisphere',
  'Brain_Cerebellum',
  'Brain_Cortex',
  'Brain_Frontal_Cortex_BA9',
  'Brain_Hippocampus',
  'Brain_Hypothalamus', 
  'Brain_Nucleus_accumbens_basal_ganglia',
  'Brain_Putamen_basal_ganglia',
  'Brain_Spinal_cord_cervical_c-1',
  'Brain_Substantia_nigra',
  'Breast_Mammary_Tissue',
  'Cells_EBV-transformed_lymphocytes',
  'Cells_Transformed_fibroblasts',
  'Colon_Sigmoid',
  'Colon_Transverse',
  'Esophagus_Gastroesophageal_Junction',
  'Esophagus_Mucosa',
  'Esophagus_Muscularis',
  'Heart_Atrial_Appendage',
  'Heart_Left_Ventricle',
  'Liver',
  'Lung',
  'Minor_Salivary_Gland',
  'Muscle_Skeletal',
  'Nerve_Tibial',
  'Ovary',
  'Pancreas',
  'Pituitary',
  'Prostate',
  'Skin_Not_Sun_Exposed_Suprapubic',
  'Skin_Sun_Exposed_Lower_leg',
  'Small_Intestine_Terminal_Ileum',
  'Spleen',
  'Stomach',
  'Testis',
  'Thyroid',
  'Uterus',
  'Vagina',
  'Whole_Blood'
]

ht_genes = hl.import_table('gs://hail-datasets/raw-data/gtex/v7/reference/gencode.v19.genes.v7.patched_contigs.gtf',
                           comment='#', no_header=True, types={'f3': hl.tint, 'f4': hl.tint}, missing='.', min_partitions=12)
ht_genes = ht_genes.rename({'f0': 'contig',
                            'f1': 'annotation_source',
                            'f2': 'feature_type',
                            'f3': 'start',
                            'f4': 'end',
                            'f5': 'score',
                            'f6': 'strand',
                            'f7': 'phase',
                            'f8': 'attributes'})
ht_genes = ht_genes.filter(ht_genes.feature_type == 'gene')
ht_genes = ht_genes.annotate(interval=hl.interval(hl.locus(ht_genes.contig, ht_genes.start, 'GRCh37'), hl.locus(ht_genes.contig, ht_genes.end + 1, 'GRCh37')))
ht_genes = ht_genes.annotate(attributes=hl.dict(hl.map(lambda x: (x.split(' ')[0], x.split(' ')[1].replace('"', '').replace(';$', '')), ht_genes.attributes.split('; '))))
attribute_cols = list(ht_genes.aggregate(hl.set(hl.flatten(hl.agg.collect(ht_genes.attributes.keys())))))
ht_genes = ht_genes.annotate(**{x: hl.or_missing(ht_genes.attributes.contains(x), ht_genes.attributes[x]) for x in attribute_cols})
ht_genes = ht_genes.select(*(['gene_id', 'interval', 'gene_type', 'strand', 'annotation_source', 'havana_gene', 'gene_status', 'tag']))
ht_genes = ht_genes.rename({'havana_gene': 'havana_gene_id'})
ht_genes = ht_genes.key_by(ht_genes.gene_id)

ht_samples = hl.import_table('gs://hail-datasets/raw-data/gtex/v7/annotations/GTEx_v7_Annotations.SampleAttributesDS.txt', 
                             key='SAMPID',
                             missing='')

float_cols = ['SMRIN',
              'SME2MPRT',
              'SMNTRART',
              'SMMAPRT',
              'SMEXNCRT',
              'SM550NRM',
              'SMUNMPRT',
              'SM350NRM',
              'SMMNCPB',
              'SME1MMRT',
              'SMNTERRT',
              'SMMNCV',
              'SMGAPPCT',
              'SMNTRNRT',
              'SMMPUNRT',
              'SMEXPEFF',
              'SME2MMRT',
              'SMBSMMRT',
              'SME1PCTS',
              'SMRRNART',
              'SME1MPRT',
              'SMDPMPRT',
              'SME2PCTS']

int_cols = ['SMTSISCH',
            'SMATSSCR',
            'SMTSPAX',
            'SMCHMPRS',
            'SMNUMGPS',
            'SMGNSDTC',
            'SMRDLGTH',
            'SMSFLGTH',
            'SMESTLBS',
            'SMMPPD',
            'SMRRNANM',
            'SMVQCFL',
            'SMTRSCPT',
            'SMMPPDPR',
            'SMCGLGTH',
            'SMUNPDRD',
            'SMMPPDUN',
            'SME2ANTI',
            'SMALTALG',
            'SME2SNSE',
            'SMMFLGTH',
            'SMSPLTRD',
            'SME1ANTI',
            'SME1SNSE',
            'SMNUM5CD']

ht_samples = ht_samples.annotate(**{x: hl.float(ht_samples[x]) for x in float_cols})
ht_samples = ht_samples.annotate(**{x: hl.int(ht_samples[x].replace('.0$', '')) for x in int_cols})

hts_covariates = {}
for t in tissues:
    ht = hl.import_table('gs://hail-datasets/raw-data/gtex/v7/single-tissue-eqtl/processed/{}.v7.covariates.T.tsv.bgz'.format(t),
                         key='ID', impute=True)
    ht = ht.rename({'ID': 'sample_id'})
    hts_covariates[t] = ht

mt = hl.import_matrix_table('gs://hail-datasets/raw-data/gtex/v7/single-tissue-eqtl/processed/{}.v7.normalized_expression.tsv.bgz'.format(tissues[0]),
                            row_fields={'#chr': hl.tstr, 'start': hl.tint, 'end': hl.tint, 'gene_id': hl.tstr},
                            row_key='gene_id',
                            entry_type=hl.tfloat,
                            min_partitions=10)
mt = mt.annotate_rows(interval=hl.interval(hl.locus(mt['#chr'], mt['start'], 'GRCh37'), hl.locus(mt['#chr'], mt['end'] + 1, 'GRCh37')))
mt = mt.select_rows('interval')
mt = mt.select_entries(normalized_gene_expression=hl.struct(**{tissues[0]: mt.x}))
mt = mt.rename({'col_id': 'sample_id'})

for t in tissues[1:]:
    mt_t = hl.import_matrix_table('gs://hail-datasets/raw-data/gtex/v7/single-tissue-eqtl/processed/{}.v7.normalized_expression.tsv.bgz'.format(t),
                                  row_fields={'#chr': hl.tstr, 'start': hl.tint, 'end': hl.tint, 'gene_id': hl.tstr},
                                  row_key='gene_id',
                                  entry_type=hl.tfloat,
                                  min_partitions=10)
    mt_t = mt_t.annotate_rows(interval=hl.interval(hl.locus(mt_t['#chr'], mt_t['start'], 'GRCh37'), hl.locus(mt_t['#chr'], mt_t['end'] + 1, 'GRCh37')))
    mt_t = mt_t.select_rows('interval')
    mt_t = mt_t.rename({'col_id': 'sample_id'})
    mt = mt.annotate_entries(normalized_gene_expression=mt.normalized_gene_expression.annotate(**{t: mt_t[mt.gene_id, mt.sample_id].x}))

mt = mt.annotate_rows(**ht_genes[mt.gene_id])
mt = mt.annotate_cols(sample_attributes=hl.struct(**ht_samples[mt.sample_id]))
mt = mt.annotate_cols(eqtl_covariates=hl.struct(**{t: hl.struct(**hts_covariates[t][mt.sample_id]) for t in tissues}))

mt.describe()
mt.write('gs://hail-datasets/hail-data/gtex_v7_eqtl_normalized_expression_and_covariates.GRCh37.mt', overwrite=True)
