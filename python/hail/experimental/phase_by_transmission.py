import hail as hl
from hail.typecheck import *
from hail.expr.expressions import *
from hail.matrixtable import MatrixTable
from typing import List


@typecheck(locus=expr_locus,
           alleles=expr_array(expr_str),
           proband_call=expr_call,
           father_call=expr_call,
           mother_call=expr_call)
def phase_by_transmission(
        locus: hl.expr.LocusExpression,
        alleles: hl.expr.ArrayExpression,
        proband_call: hl.expr.CallExpression,
        father_call: hl.expr.CallExpression,
        mother_call: hl.expr.CallExpression
) -> hl.expr.ArrayExpression:
    """
    Phases genotype calls in a trio based allele transmission.

    In the phased calls returned, the order is as follows:
    * Proband: father_allele | mother_allele
    * Parents: transmitted_allele | untransmitted_allele

    Phasing of sex chromosomes:
    Sex chromosomes of male individuals should be haploid to be phased correctly.
    If `proband_call` is diploid on non-par regions of the sex chromosomes, it is assumed to be female.

    Returns `NA` when genotype calls cannot be phased.
    The following genotype calls combinations cannot be phased by transmission:
    1. One of the calls in the trio is missing
    2. The proband genotype cannot be obtained from the parents alleles (Mendelian violation)
    3. All individuals of the trio are heterozygous for the same two alleles
    4. Father is diploid on non-PAR region of X or Y
    5. Proband is diploid on non-PAR region of Y

    In addition, individual phased genotype calls are returned as missing in the following situations:
    1. All mother genotype calls non-PAR region of Y
    2. Diploid father genotype calls on non-PAR region of X for a male proband (proband and mother are still phased as father doesn't participate in allele transmission)

    :param LocusExpression locus: Locus in the trio MatrixTable
    :param ArrayExpression alleles: Alleles in the trio MatrixTable
    :param CallExpression proband_call: Input proband genotype call
    :param CallExpression father_call: Input father genotype call
    :param CallExpression mother_call: Input mother genotype call
    :return: Array containing: phased proband call, phased father call, phased mother call
    :rtype: ArrayExpression
    """

    def call_to_one_hot_alleles_array(call: hl.expr.CallExpression, alleles: hl.expr.ArrayExpression) -> hl.expr.ArrayExpression:
        """
        Get the set of all different one-hot-encoded allele-vectors in a genotype call.
        It is returned as an ordered array where the first vector corresponds to the first allele,
        and the second vector (only present if het) the second allele.

        :param CallExpression call: genotype
        :param ArrayExpression alleles: Alleles at the site
        :return: Array of one-hot-encoded alleles
        :rtype: ArrayExpression
        """
        return hl.cond(
            call.is_het(),
            hl.array([
                hl.call(call[0]).one_hot_alleles(alleles),
                hl.call(call[1]).one_hot_alleles(alleles),
            ]),
            hl.array([hl.call(call[0]).one_hot_alleles(alleles)])
        )

    def phase_parent_call(call: hl.expr.CallExpression, transmitted_allele_index: int):
        """
        Given a genotype and which allele was transmitted to the offspring, returns the parent phased genotype.

        :param CallExpression call: Parent genotype
        :param int transmitted_allele_index: index of transmitted allele (0 or 1)
        :return: Phased parent genotype
        :rtype: CallExpression
        """
        return hl.call(
            call[transmitted_allele_index],
            call[hl.int(transmitted_allele_index == 0)],
            phased=True
        )

    def phase_diploid_proband(
            locus: hl.expr.LocusExpression,
            alleles: hl.expr.ArrayExpression,
            proband_call: hl.expr.CallExpression,
            father_call: hl.expr.CallExpression,
            mother_call: hl.expr.CallExpression
    ) -> hl.expr.ArrayExpression:
        """
        Returns phased genotype calls in the case of a diploid proband
        (autosomes, PAR regions of sex chromosomes or non-PAR regions of a female proband)

        :param LocusExpression locus: Locus in the trio MatrixTable
        :param ArrayExpression alleles: Alleles in the trio MatrixTable
        :param CallExpression proband_call: Input proband genotype call
        :param CallExpression father_call: Input father genotype call
        :param CallExpression mother_call: Input mother genotype call
        :return: Array containing: phased proband call, phased father call, phased mother call
        :rtype: ArrayExpression
        """

        proband_v = proband_call.one_hot_alleles(alleles)
        father_v = hl.cond(
            locus.in_x_nonpar() | locus.in_y_nonpar(),
            hl.or_missing(father_call.is_haploid(), hl.array([father_call.one_hot_alleles(alleles)])),
            call_to_one_hot_alleles_array(father_call, alleles)
        )
        mother_v = call_to_one_hot_alleles_array(mother_call, alleles)

        combinations = hl.flatmap(
            lambda f:
            hl.zip_with_index(mother_v)
                .filter(lambda m: m[1] + f[1] == proband_v)
                .map(lambda m: hl.struct(m=m[0], f=f[0])),
            hl.zip_with_index(father_v)
        )

        return (
            hl.or_missing(
                hl.is_defined(combinations) & (hl.len(combinations) == 1),
                hl.array([
                    hl.call(father_call[combinations[0].f], mother_call[combinations[0].m], phased=True),
                    hl.cond(father_call.is_haploid(), hl.call(father_call[0], phased=True), phase_parent_call(father_call, combinations[0].f)),
                    phase_parent_call(mother_call, combinations[0].m)
                ])
            )
        )

    def phase_haploid_proband_x_nonpar(
            proband_call: hl.expr.CallExpression,
            father_call: hl.expr.CallExpression,
            mother_call: hl.expr.CallExpression
    ) -> hl.expr.ArrayExpression:
        """
        Returns phased genotype calls in the case of a haploid proband in the non-PAR region of X

        :param CallExpression proband_call: Input proband genotype call
        :param CallExpression father_call: Input father genotype call
        :param CallExpression mother_call: Input mother genotype call
        :return: Array containing: phased proband call, phased father call, phased mother call
        :rtype: ArrayExpression
        """

        transmitted_allele = hl.zip_with_index(hl.array([mother_call[0], mother_call[1]])).find(lambda m: m[1] == proband_call[0])
        return hl.or_missing(
            hl.is_defined(transmitted_allele),
            hl.array([
                hl.call(proband_call[0], phased=True),
                hl.or_missing(father_call.is_haploid(), hl.call(father_call[0], phased=True)),
                phase_parent_call(mother_call, transmitted_allele[0])
            ])
        )

    def phase_y_nonpar(
            proband_call: hl.expr.CallExpression,
            father_call: hl.expr.CallExpression,
    ) -> hl.expr.ArrayExpression:
        """
        Returns phased genotype calls in the non-PAR region of Y (requires both father and proband to be haploid to return phase)

        :param CallExpression proband_call: Input proband genotype call
        :param CallExpression father_call: Input father genotype call
        :return: Array containing: phased proband call, phased father call, phased mother call
        :rtype: ArrayExpression
        """
        return hl.or_missing(
            proband_call.is_haploid() & father_call.is_haploid() & (father_call[0] == proband_call[0]),
            hl.array([
                hl.call(proband_call[0], phased=True),
                hl.call(father_call[0], phased=True),
                hl.null(hl.tcall)
            ])
        )

    return (
        hl.case()
            .when(locus.in_x_nonpar() & proband_call.is_haploid(), phase_haploid_proband_x_nonpar(proband_call, father_call, mother_call))
            .when(locus.in_y_nonpar(), phase_y_nonpar(proband_call, father_call))
            .when(proband_call.is_diploid(), phase_diploid_proband(locus, alleles, proband_call, father_call, mother_call))
            .or_missing()
    )


@typecheck(tm=MatrixTable,
           call_field=str,
           phased_call_field=str)
def phase_trio_matrix_by_transmission(tm: hl.MatrixTable, call_field: str = 'GT', phased_call_field: str = 'PBT_GT') -> hl.MatrixTable:
    """
        Adds a phased genoype entry to a trio MatrixTable based allele transmission in the trio.
        Uses only a `Call` field to phase and only phases when all 3 members of the trio are present and have a call.

        In the phased genotypes, the order is as follows:
        * Proband: father_allele | mother_allele
        * Parents: transmitted_allele | untransmitted_allele

        Phasing of sex chromosomes:
        Sex chromosomes of male individuals should be haploid to be phased correctly.
        If a proband is diploid on non-par regions of the sex chromosomes, it is assumed to be female.

        Genotypes that cannot be phased are set to `NA`.
        The following genotype calls combinations cannot be phased by transmission (all trio members phased calls set to missing):
        1. One of the calls in the trio is missing
        2. The proband genotype cannot be obtained from the parents alleles (Mendelian violation)
        3. All individuals of the trio are heterozygous for the same two alleles
        4. Father is diploid on non-PAR region of X or Y
        5. Proband is diploid on non-PAR region of Y

        In addition, individual phased genotype calls are returned as missing in the following situations:
        1. All mother genotype calls non-PAR region of Y
        2. Diploid father genotype calls on non-PAR region of X for a male proband (proband and mother are still phased as father doesn't participate in allele transmission)


        Typical usage:
        ```
            trio_matrix = hl.trio_matrix(mt, ped)
            phased_trio_matrix = phase_trio_matrix_by_transmission(trio_matrix)
        ```

        :param MatrixTable tm: Trio MatrixTable (entries should be a Struct with `proband_entry`, `mother_entry` and `father_entry` present)
        :param str call_field: genotype field name to phase
        :param str phased_call_field: name for the phased genotype field
        :return: trio MatrixTable entry with additional phased genotype field for each individual
        :rtype: MatrixTable
        """

    tm = tm.annotate_entries(
        __phased_GT=phase_by_transmission(
            tm.locus,
            tm.alleles,
            tm.proband_entry[call_field],
            tm.father_entry[call_field],
            tm.mother_entry[call_field]
        )
    )

    return tm.select_entries(
        proband_entry=hl.struct(
            **tm.proband_entry,
            **{phased_call_field: tm.__phased_GT[0]}
        ),
        father_entry=hl.struct(
            **tm.father_entry,
            **{phased_call_field: tm.__phased_GT[1]}
        ),
        mother_entry=hl.struct(
            **tm.mother_entry,
            **{phased_call_field: tm.__phased_GT[2]}
        )
    )


@typecheck(tm=MatrixTable,
           col_keys=sequenceof(str))
def explode_trio_matrix(tm: hl.MatrixTable, col_keys: List[str] = ['s']) -> hl.MatrixTable:
    """

    Splits a trio MatrixTable back into a sample MatrixTable.
    This assumes that the input MatrixTable is a trio MatrixTable (similar to the result of `hail.methods.trio_matrix`)
    In particular, it should have the following entry schema:
    * proband_entry
    * father_entry
    * mother_entry
    And the following column schema:
    * proband
    * father
    * mother

    Note that all other entry and column fields will be dropped.

    :param MatrixTable tm: Input trio MatrixTable
    :param list of str col_keys: Column keys for the sample MatrixTable
    :return: Sample MatrixTable
    :rtype: MatrixTable
    """
    tm = tm.select_entries(
        __trio_entries=hl.array([tm.proband_entry, tm.father_entry, tm.mother_entry])
    )

    tm = tm.select_cols(
        __trio_members=hl.zip_with_index(hl.array([tm.proband, tm.father, tm.mother]))
    )
    mt = tm.explode_cols(tm.__trio_members)

    mt = mt.select_entries(
        **mt.__trio_entries[mt.__trio_members[0]]
    )

    mt = mt.key_cols_by()
    mt = mt.select_cols(**mt.__trio_members[1])

    if col_keys:
        mt = mt.key_cols_by(*col_keys)

    return mt
