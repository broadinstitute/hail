
import hail as hl


def import_gtf(path, reference_genome=None, skip_invalid_contigs=False, min_partitions=None) -> hl.Table:
    """Import a GTF file.

       The GTF file format is identical to the GFF version 2 file format,
       and so this function can be used to import GFF version 2 files as
       well.

       See https://www.ensembl.org/info/website/upload/gff.html for more
       details on the GTF/GFF2 file format.

       The :class:`.Table` returned by this function will be keyed by the
       ``interval`` row field and will include the following row fields:

       .. code-block:: text

           'source': str
           'feature': str
           'score': float64
           'strand': str
           'frame': int32
           'interval': interval<>

       There will also be corresponding fields for every tag found in the
       attribute field of the GTF file.

       Note
       ----

       This function will return an ``interval`` field of type :class:`.tinterval`
       constructed from the ``seqname``, ``start``, and ``end`` fields in the
       GTF file. This interval is inclusive of both the start and end positions
       in the GTF file. 

       If the ``reference_genome`` parameter is specified, the start and end
       points of the ``interval`` field will be of type :class:`.tlocus`.
       Otherwise, the start and end points of the ``interval`` field will be of
       type :class:`.tstruct` with fields ``seqname`` (type :class:`str`) and
       ``position`` (type :class:`.tint32`).

       Furthermore, if the ``reference_genome`` parameter is specified and
       ``skip_invalid_contigs`` is ``True``, this import function will skip
       lines in the GTF where ``seqname`` is not consistent with the reference
       genome specified.

       Example
       -------

       >>> ht = hl.experimental.import_gtf('data/test.gtf', 
       ...                                 reference_genome='GRCh37',
       ...                                 skip_invalid_contigs=True)

       >>> ht.describe()  # doctest: +NOTEST
       ----------------------------------------
       Global fields:
       None
       ----------------------------------------
       Row fields:
           'source': str
           'feature': str
           'score': float64
           'strand': str
           'frame': int32
           'gene_type': str
           'exon_id': str
           'havana_transcript': str
           'level': str
           'transcript_name': str
           'gene_status': str
           'gene_id': str
           'transcript_type': str
           'tag': str
           'transcript_status': str
           'gene_name': str
           'transcript_id': str
           'exon_number': str
           'havana_gene': str
           'interval': interval<locus<GRCh37>>
       ----------------------------------------
       Key: ['interval']
       ----------------------------------------

       Parameters
       ----------

       path : :obj:`str`
           File to import.
       reference_genome : :obj:`str` or :class:`.ReferenceGenome`, optional
           Reference genome to use.
       skip_invalid_contigs : :obj:`bool`
           If ``True`` and `reference_genome` is not ``None``, skip lines where
           ``seqname`` is not consistent with the reference genome.
       min_partitions : :obj:`int` or :obj:`None`
           Minimum number of partitions (passed to import_table).

       Returns
       -------
       :class:`.Table`
       """

    ht = hl.import_table(path,
                         min_partitions=min_partitions,
                         comment='#',
                         no_header=True,
                         types={'f3': hl.tint,
                                'f4': hl.tint,
                                'f5': hl.tfloat,
                                'f7': hl.tint},
                         missing='.',
                         delimiter='\t')

    ht = ht.rename({'f0': 'seqname',
                    'f1': 'source',
                    'f2': 'feature',
                    'f3': 'start',
                    'f4': 'end',
                    'f5': 'score',
                    'f6': 'strand',
                    'f7': 'frame',
                    'f8': 'attribute'})

    ht = ht.annotate(attribute=hl.dict(
        hl.map(lambda x: (x.split(' ')[0],
                          x.split(' ')[1].replace('"', '').replace(';$', '')),
               ht['attribute'].split('; '))))

    attributes = ht.aggregate(hl.agg.explode(lambda x: hl.agg.collect_as_set(x), ht['attribute'].keys()))

    ht = ht.transmute(**{x: hl.or_missing(ht['attribute'].contains(x),
                                          ht['attribute'][x])
                         for x in attributes if x})

    if reference_genome:
        if reference_genome == 'GRCh37':
            ht = ht.annotate(seqname=ht['seqname'].replace('^chr', ''))
        else:
            ht = ht.annotate(seqname=hl.case()
                                       .when(ht['seqname'].startswith('HLA'), ht['seqname'])
                                       .when(ht['seqname'].startswith('chrHLA'), ht['seqname'].replace('^chr', ''))
                                       .when(ht['seqname'].startswith('chr'), ht['seqname'])
                                       .default('chr' + ht['seqname']))
        if skip_invalid_contigs:
            valid_contigs = hl.literal(set(hl.get_reference(reference_genome).contigs))
            ht = ht.filter(valid_contigs.contains(ht['seqname']))
        ht = ht.transmute(interval=hl.locus_interval(ht['seqname'],
                                                     ht['start'],
                                                     ht['end'],
                                                     includes_start=True,
                                                     includes_end=True,
                                                     reference_genome=reference_genome))
    else:
        ht = ht.transmute(interval=hl.interval(hl.struct(seqname=ht['seqname'], position=ht['start']),
                                               hl.struct(seqname=ht['seqname'], position=ht['end']),
                                               includes_start=True,
                                               includes_end=True))

    ht = ht.key_by('interval')

    return ht


def get_gene_intervals(gene_symbols=None, gene_ids=None, transcript_ids=None,
                       verbose=True, gtf_file='gs://hail-common/references/gencode/gencode.v19.annotation.gtf.bgz',
                       reference_genome=None):
    """Get intervals of genes or transcripts.

       Get the boundaries of genes or transcripts from a GTF file, for quick filtering of a Table or MatrixTable.

       Gencode v19 (GRCh37) GTF available at: gs://hail-common/references/gencode/gencode.v19.annotation.gtf.bgz
       Gencode v29 (GRCh38) GTF available at: gs://hail-common/references/gencode/gencode.v29.annotation.gtf.bgz

       Example
       -------
       >>> hl.filter_intervals(ht, get_gene_intervals(gene_symbols=['PCSK9']))  # doctest: +SKIP

       Parameters
       ----------

       gene_symbols : :obj:`list` of :obj:`str`, optional
           Gene symbols (e.g. PCSK9).
       gene_ids : :obj:`list` of :obj:`str`, optional
           Gene IDs (e.g. ENSG00000223972).
       transcript_ids : :obj:`list` of :obj:`str`, optional
           Transcript IDs (e.g. ENSG00000223972).
       verbose : :obj:`bool`
           If ``True``, print which genes and transcripts were matched in the GTF file.
       gtf_file : :obj:`str`
           GTF file to load.
       reference_genome : :obj:`str` or :class:`.ReferenceGenome`, optional
           Reference genome to use (passed along to import_gtf).

       Returns
       -------
       :obj:`list` of :class:`.Interval`
    """
    if not gene_symbols and not gene_ids and not transcript_ids:
        raise ValueError('get_gene_intervals requires at least one of gene_symbols, gene_ids, or transcript_ids')
    ht = hl.experimental.import_gtf(gtf_file, reference_genome=reference_genome,
                                    skip_invalid_contigs=True, min_partitions=12)
    ht = ht.annotate(gene_id=ht.gene_id.split(f'\\.')[0],
                     transcript_id=ht.transcript_id.split('\\.')[0])
    criteria = [lambda x: False]
    if gene_symbols:
        criteria.append(lambda x: hl.any(lambda y: (x.feature == 'gene') & (x.gene_name == y), gene_symbols))
    if gene_ids:
        criteria.append(lambda x: hl.any(lambda y: (x.feature == 'gene') & (x.gene_id == y.split('\\.')[0]), gene_ids))
    if transcript_ids:
        criteria.append(lambda x: hl.any(lambda y: (x.feature == 'transcript') & (x.transcript_id == y.split('\\.')[0]), transcript_ids))

    def combine_functions(func_list, x, operator='and'):
        possible_operators = ('and', 'or')
        if operator not in possible_operators:
            raise ValueError(f'combine_functions only allows operators: {", ".join(possible_operators)}')
        cond = func_list[0](x)
        for c in func_list[1:]:
            if operator == 'and':
                cond &= c(x)
            elif operator == 'or':
                cond |= c(x)
        return cond

    ht = ht.filter(combine_functions(criteria, ht, operator='or'))
    gene_info = ht.aggregate(hl.agg.collect((ht.feature, ht.gene_name, ht.gene_id, ht.transcript_id, ht.interval)))
    if verbose:
        print(f'get_gene_intervals found {len(gene_info)} entries:\n' +
              "\n".join(map(lambda x: f'{x[0]}: {x[1]} ({x[2] if x[0] == "gene" else x[3]})', gene_info)))
    intervals = list(map(lambda x: x[-1], gene_info))
    return intervals
