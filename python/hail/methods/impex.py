from hail.typecheck import *
from hail.utils.java import Env, handle_py4j, joption, FatalError
from hail.api2 import Table, MatrixTable
from hail.expr.types import *
from hail.expr.expression import analyze, expr_any
from hail.genetics import GenomeReference
from hail.methods.misc import require_biallelic

@handle_py4j
@typecheck(table=Table,
           address=strlike,
           keyspace=strlike,
           table_name=strlike,
           block_size=integral,
           rate=integral)
def export_cassandra(table, address, keyspace, table_name, block_size=100, rate=1000):
    """Export to Cassandra.

    Warning
    -------
    :func:`export_cassandra` is EXPERIMENTAL.
    """

    table._jkt.exportCassandra(address, keyspace, table_name, block_size, rate)

@handle_py4j
@typecheck(dataset=MatrixTable,
           output=strlike,
           precision=integral)
def export_gen(dataset, output, precision=4):
    """Export variant dataset as GEN and SAMPLE files.

    .. include:: ../_templates/req_tvariant.rst

    .. include:: ../_templates/req_biallelic.rst

    Examples
    --------
    Import genotype probability data, filter variants based on INFO score, and
    export data to a GEN and SAMPLE file:
    
    >>> ds = hc.import_gen('data/example.gen', sample_file='data/example.sample')
    >>> ds = ds.filter_rows(agg.infoScore(ds.GP).score >= 0.9) # doctest: +SKIP
    >>> methods.export_gen(ds, 'output/infoscore_filtered')

    Notes
    -----
    Writes out the dataset to a GEN and SAMPLE fileset in the
    `Oxford spec <http://www.stats.ox.ac.uk/%7Emarchini/software/gwas/file_format.html>`__.

    This method requires a `GP` (genotype probabilities) entry field of type
    ``Array[Float64]``. The values at indices 0, 1, and 2 are exported as the
    probabilities of homozygous reference, heterozygous, and homozygous variant,
    respectively. Missing `GP` values are exported as ``0 0 0``.

    The first six columns of the GEN file are as follows:

    - chromosome (`v.contig`)
    - variant ID (`varid` if defined, else Contig:Position:Ref:Alt)
    - rsID (`rsid` if defined, else ``.``)
    - position (`v.start`)
    - reference allele (`v.ref`)
    - alternate allele (`v.alt`)

    The SAMPLE file has three columns:

    - ID_1 and ID_2 are identical and set to the sample ID (`s`).
    - The third column (``missing``) is set to 0 for all samples.

    Parameters
    ----------
    dataset : :class:`.MatrixTable`
        Dataset with entry field `GP` of type Array[TFloat64].
    output : :obj:`str`
        Filename root for output GEN and SAMPLE files.
    precision : :obj:`int`
        Number of digits to write after the decimal point.
    """

    dataset = require_biallelic(dataset, 'export_gen')
    try:
        gp = dataset['GP']
        if gp.dtype != TArray(TFloat64()) or gp._indices != dataset._entry_indices:
            raise KeyError
    except KeyError:
        raise FatalError("export_gen: no entry field 'GP' of type Array[Float64]")
    
    dataset = require_biallelic(dataset, 'export_plink')
    
    Env.hail().io.gen.ExportGen.apply(dataset._jvds, output, precision)

@handle_py4j
@typecheck(dataset=MatrixTable,
           output=strlike,
           fam_args=expr_any)
def export_plink(dataset, output, **fam_args):
    """Export variant dataset as
    `PLINK2 <https://www.cog-genomics.org/plink2/formats>`__
    BED, BIM and FAM files.

    .. include:: ../_templates/req_tvariant.rst
    
    .. include:: ../_templates/req_tstring.rst

    .. include:: ../_templates/req_biallelic.rst

    Examples
    --------
    Import data from a VCF file, split multi-allelic variants, and export to
    PLINK files with the FAM file individual ID set to the sample ID:

    >>> ds = methods.split_multi_hts(dataset)
    >>> methods.export_plink(ds, 'output/example', id = ds.s)

    Notes
    -----
    `fam_args` may be used to set the fields in the output
    `FAM file <https://www.cog-genomics.org/plink2/formats#fam>`__
    via expressions with column and global fields in scope:

    - ``fam_id``: :class:`.TString` for the family ID
    - ``id``: :class:`.TString` for the individual (proband) ID
    - ``mat_id``: :class:`.TString` for the maternal ID
    - ``pat_id``: :class:`.TString` for the paternal ID
    - ``is_female``: :class:`.TBoolean` for the proband sex
    - ``is_case``: :class:`.TBoolean` or `quant_pheno`: :class:`.TFloat64` for the
       phenotype

    If no assignment is given, the corresponding PLINK missing value is written:
    ``0`` for IDs and sex, ``NA`` for phenotype. Only one of ``is_case`` or
    ``quant_pheno`` can be assigned. For Boolean expressions, true and false are
    output as ``2`` and ``1``, respectively (i.e., female and case are ``2``).

    The BIM file ID field has the form ``chr:pos:ref:alt`` with values given by
    `v.contig`, `v.start`, `v.ref`, and `v.alt`.

    On an imported VCF, the example above will behave similarly to the PLINK
    conversion command

    .. code-block:: text

        plink --vcf /path/to/file.vcf --make-bed --out sample --const-fid --keep-allele-order

    except that:

    - Variants that result from splitting a multi-allelic variant may be
      re-ordered relative to the BIM and BED files.
    - PLINK uses the rsID for the BIM file ID.

    Parameters
    ----------
    dataset : :class:`.MatrixTable`
        Dataset.
    output : :obj:`str`
        Filename root for output BED, BIM, and FAM files.
    fam_args : varargs of :class:`hail.expr.expression.Expression`
        Named expressions defining FAM field values.
    """
    
    fam_dict = {'fam_id': TString(), 'id': TString(), 'mat_id': TString(), 'pat_id': TString(),
                'is_female': TBoolean(), 'is_case': TBoolean(), 'quant_pheno': TFloat64()}
        
    exprs = []
    named_exprs = {k: v for k, v in fam_args.items()}
    if ('is_case' in named_exprs) and ('quant_pheno' in named_exprs):
        raise ValueError("At most one of 'is_case' and 'quant_pheno' may be given as fam_args. Found both.")
    for k, v in named_exprs.items():
        if k not in fam_dict:
            raise ValueError("fam_arg '{}' not recognized. Valid names: {}".format(k, ', '.join(fam_dict)))
        elif (v.dtype != fam_dict[k]):
            raise TypeError("fam_arg '{}' expression has type {}, expected type {}".format(k, v.dtype, fam_dict[k]))
        
        analyze('export_plink/{}'.format(k), v, dataset._col_indices)
        exprs.append('`{k}` = {v}'.format(k=k, v=v._ast.to_hql()))
    base, _ = dataset._process_joins(*named_exprs.values())
    base = require_biallelic(base, 'export_plink')

    Env.hail().io.plink.ExportPlink.apply(base._jvds, output, ','.join(exprs))

@handle_py4j
@typecheck(table=Table,
           zk_host=strlike,
           collection=strlike,
           block_size=integral)
def export_solr(table, zk_host, collection, block_size=100):
    """Export to Solr.
    
    Warning
    -------
    :func:`export_solr` is EXPERIMENTAL.
    """

    table._jkt.exportSolr(zk_host, collection, block_size)

@handle_py4j
@typecheck(dataset=MatrixTable,
           output=strlike,
           append_to_header=nullable(strlike),
           parallel=nullable(enumeration('separate_header', 'header_per_shard')),
           metadata=nullable(dictof(strlike, dictof(strlike, dictof(strlike, strlike)))))
def export_vcf(dataset, output, append_to_header=None, parallel=None, metadata=None):
    """Export variant dataset as a VCF file in ``.vcf`` or ``.vcf.bgz`` format.

    .. include:: ../_templates/req_tvariant.rst

    Examples
    --------
    Export to VCF as a block-compressed file:

    >>> methods.export_vcf(dataset, 'output/example.vcf.bgz')

    Notes
    -----
    :func:`export_vcf` writes the dataset to disk in VCF format as described in the
    `VCF 4.2 spec <https://samtools.github.io/hts-specs/VCFv4.2.pdf>`__.

    Use the ``.vcf.bgz`` extension rather than ``.vcf`` in the output file name
    for `blocked GZIP <http://www.htslib.org/doc/tabix.html>`__ compression.

    Note
    ----
        We strongly recommended compressed (``.bgz`` extension) and parallel
        output (`parallel` set to ``'separate_header'`` or
        ``'header_per_shard'``) when exporting large VCFs.

    Hail exports the fields of Struct `info` as INFO fields,
    the elements of Set[String] `filters` as FILTERS, and the
    value of Float64 `qual` as QUAL. No other row fields are exported.

    The FORMAT field is generated from the entry schema, which
    must be a :class:`~hail.expr.TStruct`.  There is a FORMAT
    field for each field of the Struct.

    INFO and FORMAT fields may be generated from Struct fields of type Call,
    Int32, Float32, Float64, or String. If a field has type Int64, every value
    must be a valid Int32. Arrays and Sets containing these types are also
    allowed but cannot be nested; for example, Array[Array[Int32]] is invalid.
    Sets and Arrays are written with the same comma-separated format. Boolean
    fields are also permitted in `info` and will generate INFO fields of
    VCF type Flag.

    Hail also exports the name, length, and assembly of each contig as a VCF
    header line, where the assembly is set to the :class:`.GenomeReference`
    name.

    Consider the workflow of importing a VCF and immediately exporting the
    dataset back to VCF. The output VCF header will contain FORMAT lines for
    each entry field and INFO lines for all fields in `info`, but these lines
    will have empty Description fields and the Number and Type fields will be
    determined from their corresponding Hail types. To output a desired
    Description, Number, and/or Type value in a FORMAT or INFO field or to
    specify FILTER lines, use the `metadata` parameter to supply a dictionary
    with the relevant information. See
    :class:`~hail.api2.HailContext.get_vcf_metadata` for how to obtain the
    dictionary corresponding to the original VCF, and for info on how this
    dictionary should be structured. 
    
    The output VCF header will also contain CONTIG lines
    with ID, length, and assembly fields derived from the reference genome of
    the dataset.
    
    The output VCF header will `not` contain lines added by external tools
    (such as bcftools and GATK) unless they are explicitly inserted using the
    `append_to_header` parameter.

    Warning
    -------
        INFO fields stored at VCF import are `not` automatically modified to
        reflect filtering of samples or genotypes, which can affect the value of
        AC (allele count), AF (allele frequency), AN (allele number), etc. If a
        filtered dataset is exported to VCF without updating `info`, downstream
        tools which may produce erroneous results. The solution is to create new
        annotations in `info` or overwrite existing annotations. For example, in
        order to produce an accurate `AC` field, one can run :func:`variant_qc` and
        copy the `variant_qc.AC` field to `info.AC` as shown below.
    
        >>> ds = dataset.filter_entries(dataset.GQ >= 20)
        >>> ds = methods.variant_qc(ds)
        >>> ds = ds.annotate_rows(info = ds.info.annotate(AC=ds.variant_qc.AC)) # doctest: +SKIP
        >>> methods.export_vcf(ds, 'output/example.vcf.bgz')
    
    Parameters
    ----------
    dataset : :class:`.MatrixTable`
        Dataset.
    output : :obj:`str`
        Path of .vcf or .vcf.bgz file to write.
    append_to_header : :obj:`str`, optional
        Path of file to append to VCF header.
    parallel : :obj:`str`, optional
        If ``'header_per_shard'``, return a set of VCF files (one per
        partition) rather than serially concatenating these files. If
        ``'separate_header'``, return a separate VCF header file and a set of
        VCF files (one per partition) without the header. If ``None``,
        concatenate the header and all partitions into one VCF file.
    metadata : :obj:`dict[str]` or :obj:`dict[str, dict[str, str]`, optional
        Dictionary with information to fill in the VCF header. See
        :class:`~hail.api2.HailContext.get_vcf_metadata` for how this
        dictionary should be structured.
    """

    typ = TDict(TString(), TDict(TString(), TDict(TString(), TString())))
    Env.hail().io.vcf.ExportVCF.apply(dataset._jvds, output, joption(append_to_header),
                                      Env.hail().utils.ExportType.getExportType(parallel),
                                      joption(typ._convert_to_j(metadata)))


@handle_py4j
@typecheck(path=strlike,
           reference_genome=nullable(GenomeReference))
def import_interval_list(path, reference_genome=None):
    """Import an interval list file in the GATK standard format.

    Examples
    --------

    >>> intervals = methods.import_interval_list('data/capture_intervals.txt')

    Notes
    -----

    Hail expects an interval file to contain either three or five fields per
    line in the following formats:

    - ``contig:start-end``
    - ``contig  start  end`` (tab-separated)
    - ``contig  start  end  direction  target`` (tab-separated)

    A file in either of the first two formats produces a table with one
    field:

     - **interval** (*Interval*), key field

    A file in the third format (with a "target" column) produces a table with two
    fields:

     - **interval** (*Interval*), key field
     - **target** (*String*)

    Note
    ----
    ``start`` and ``end`` match positions inclusively, e.g.
    ``start <= position <= end``. :meth:`.Interval.parse`
    is exclusive of the end position.

    Refer to :class:`.GenomeReference` for contig ordering and behavior.

    Warning
    -------
    The interval parser for these files does not support the full range of
    formats supported by the python parser
    :meth:`representation.Interval.parse`. 'k', 'm', 'start', and 'end' are all
    invalid motifs in the ``contig:start-end`` format here.

    Parameters
    ----------
    path : :obj:`str`
        Path to file.

    reference_genome : :class:`.GenomeReference`
        Reference genome to use. Default is
        :meth:`.HailContext.default_reference`.

    Returns
    -------
    :class:`.Table`
        Interval-keyed table.
    """
    rg = reference_genome if reference_genome else Env.hc().default_reference
    t = Env.hail().table.Table.importIntervalList(Env.hc()._jhc, path, rg._jrep)
    return Table(t)

@handle_py4j
@typecheck(path=strlike,
           reference_genome=nullable(GenomeReference))
def import_bed(path, reference_genome=None):
    """Import a UCSC .bed file as a :class:`.Table`.

    Examples
    --------

    >>> bed = methods.import_bed('data/file1.bed')

    >>> bed = methods.import_bed('data/file2.bed')

    The file formats are

    .. code-block:: text

        $ cat data/file1.bed
        track name="BedTest"
        20    1          14000000
        20    17000000   18000000
        ...

        $ cat file2.bed
        track name="BedTest"
        20    1          14000000  cnv1
        20    17000000   18000000  cnv2
        ...


    Notes
    -----

    The table produced by this method has one of two possible structures. If
    the .bed file has only three fields (`chrom`, `chromStart`, and
    `chromEnd`), then the produced table has only one column:

        - **interval** (*Interval*) - Genomic interval.

    If the .bed file has four or more columns, then Hail will store the fourth
    column as a field in the table:

        - **interval** (*Interval*) - Genomic interval.
        - **target** (*String*) - Fourth column of .bed file.

    `UCSC bed files <https://genome.ucsc.edu/FAQ/FAQformat.html#format1>`__ can
    have up to 12 fields, but Hail will only ever look at the first four. Hail
    ignores header lines in BED files.

    Warning
    -------
        UCSC BED files are 0-indexed and end-exclusive. The line "5  100  105"
        will contain locus ``5:105`` but not ``5:100``. Details
        `here <http://genome.ucsc.edu/blog/the-ucsc-genome-browser-coordinate-counting-systems/>`__.

    Parameters
    ----------
    path : :obj:`str`
        Path to .bed file.

    reference_genome : :class:`.GenomeReference`
        Reference genome to use. Default is
        :meth:`.HailContext.default_reference`.

    Returns
    -------
    :class:`.Table`
        Interval-indexed table containing information from file.
    """
    # FIXME: once interval join support is added, add the following examples:
    # Add the variant annotation ``va.cnvRegion: Boolean`` indicating inclusion in
    # at least one interval of the three-column BED file `file1.bed`:

    # >>> bed = methods.import_bed('data/file1.bed')
    # >>> vds_result = vds.annotate_rows(cnvRegion = bed[vds.v])

    # Add a variant annotation **va.cnvRegion** (*String*) with value given by the
    # fourth column of ``file2.bed``:

    # >>> bed = methods.import_bed('data/file2.bed')
    # >>> vds_result = vds.annotate_rows(cnvID = bed[vds.v])

    rg = reference_genome if reference_genome else Env.hc().default_reference
    jt = Env.hail().table.Table.importBED(Env.hc()._jhc, path, rg._jrep)
    return Table(jt)

@handle_py4j
@typecheck(path=strlike,
           quant_pheno=bool,
           delimiter=strlike,
           missing=strlike)
def import_fam(path, quant_pheno=False, delimiter=r'\\s+', missing='NA'):
    """Import PLINK .fam file into a key table.

    Examples
    --------

    Import a tab-separated
    `FAM file <https://www.cog-genomics.org/plink2/formats#fam>`__
    with a case-control phenotype:

    >>> fam_kt = methods.import_fam('data/case_control_study.fam')

    Import a FAM file with a quantitative phenotype:

    >>> fam_kt = methods.import_fam('data/quantitative_study.fam', quant_pheno=True)

    Notes
    -----

    In Hail, unlike PLINK, the user must *explicitly* distinguish between
    case-control and quantitative phenotypes. Importing a quantitative
    phenotype without ``quant_pheno=True`` will return an error
    (unless all values happen to be `0`, `1`, `2`, or `-9`):

    The resulting :class:`.Table` will have fields, types, and values that are interpreted as missing.

     - **fam_id** (*String*) -- Family ID (missing = "0")
     - **id** (*String*) -- Sample ID (key column)
     - **pat_id** (*String*) -- Paternal ID (missing = "0")
     - **mat_id** (*String*) -- Maternal ID (missing = "0")
     - **is_female** (*Boolean*) -- Sex (missing = "NA", "-9", "0")

    One of:

     - **is_case** (*Boolean*) -- Case-control phenotype (missing = "0", "-9",
        non-numeric or the ``missing`` argument, if given.
     - **quant_pheno** (*Float64*) -- Quantitative phenotype (missing = "NA" or the
        ``missing`` argument, if given.

    Parameters
    ----------
    path : :obj:`str`
        Path to FAM file.
    quant_pheno : :obj:`bool`
        If ``True``, phenotype is interpreted as quantitative.
    delimiter : :obj:`str`
        Field delimiter regex.
    missing : :obj:`str`
        The string used to denote missing values. For case-control, 0, -9, and
        non-numeric are also treated as missing.

    Returns
    -------
    :class:`.Table`
        Table representing the data of a FAM file.
    """

    jkt = Env.hail().table.Table.importFam(Env.hc()._jhc, path,
                                           quant_pheno, delimiter, missing)
    return Table(jkt)
