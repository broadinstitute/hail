# Change Log

## 0.2.9

Released 2019-01-30

### New features

 - (hail#5149) Added bitwise transformation functions: `hl.bit_{and, or, xor, not, lshift, rshift}`.
 - (hail#5154) Added `hl.rbind` function, which is similar to `hl.bind` but expects a function as the last argument instead of the first.
 
### Performance improvements

 - (hail#5107) Hail's Python interface generates tighter intermediate code, which should result in moderate performance improvements in many pipelines.
 - (hail#5172) Fix unintentional performance deoptimization related to `Table.show` introduced in 0.2.8.
 - (hail#5078) Improve performance of `hl.ld_prune` by up to 30x.

### Bug fixes

 - (hail#5144) Fix crash caused by `hl.index_bgen` (since 0.2.7)
 - (hail#5177) Fix bug causing `Table.repartition(n, shuffle=True)` to fail to increase partitioning for unkeyed tables.
 - (hail#5173) Fix bug causing `Table.show` to throw an error when the table is empty (since 0.2.8).
 - (hail#5210) Fix bug causing `Table.show` to always print types, regardless of `types` argument (since 0.2.8).
 - (hail#5211) Fix bug causing `MatrixTable.make_table` to unintentionally discard non-key row fields (since 0.2.8).
 
-----

## 0.2.8

Released 2019-01-15

### New features

 - (hail#5072) Added multi-phenotype option to `hl.logistic_regression_rows`
 - (hail#5077) Added support for importing VCF floating-point FORMAT fields as `float32` as well as `float64`. 

### Performance improvements

 - (hail#5068) Improved optimization of `MatrixTable.count_cols`.
 - (hail#5131) Fixed performance bug related to `hl.literal` on large values with missingness

### Bug fixes

 - (hail#5088) Fixed name separator in `MatrixTable.make_table`.
 - (hail#5104) Fixed optimizer bug related to experimental functionality.
 - (hail#5122) Fixed error constructing `Table` or `MatrixTable` objects with fields with certain character patterns like `$`.

-----

## 0.2.7

Released 2019-01-03

### New features

 - (hail#5046)(experimental) Added option to BlockMatrix.export_rectangles to export as NumPy-compatible binary.

### Performance improvements

 - (hail#5050) Short-circuit iteration in `logistic_regression_rows` and `poisson_regression_rows` if NaNs appear.

-----

## 0.2.6

Released 2018-12-17

### New features

 - (hail#4962) Expanded comparison operators (`==`, `!=`, `<`, `<=`, `>`, `>=`) to support expressions of every type.
 - (hail#4927) Expanded functionality of `Table.order_by` to support ordering by arbitrary expressions, instead of just top-level fields.
 - (hail#4926) Expanded default GRCh38 contig recoding behavior in `import_plink`.
  
### Performance improvements

 - (hail#4952) Resolved lingering issues related to (hail#4909).

### Bug fixes

 - (hail#4941) Fixed variable scoping error in regression methods.
 - (hail#4857) Fixed bug in maximal_independent_set appearing when nodes were named something other than `i` and `j`.
 - (hail#4932) Fixed possible error in `export_plink` related to tolerance of writer process failure.
 - (hail#4920) Fixed bad error message in `Table.order_by`.
 
-----

## 0.2.5 

Released 2018-12-07

### New features

 - (hail#4845) The [or_error](https://hail.is/docs/0.2/functions/core.html#hail.expr.builders.CaseBuilder.or_error) method in `hl.case` and `hl.switch` statements now takes a string expression rather than a string literal, allowing more informative messages for errors and assertions.
 - (hail#4865) We use this new `or_error` functionality in methods that require biallelic variants to include an offending variant in the error message.
 - (hail#4820) Added [hl.reversed](https://hail.is/docs/0.2/functions/collections.html?highlight=reversed#hail.expr.functions.reversed) for reversing arrays and strings.
 - (hail#4895) Added `include_strand` option to the [hl.liftover](https://hail.is/docs/0.2/functions/genetics.html?highlight=liftover#hail.expr.functions.liftover) function.


### Performance improvements
 
 - (hail#4907)(hail#4911) Addressed one aspect of bad scaling in enormous literal values (triggered by a list of 300,000 sample IDs) related to logging.
 - (hail#4909)(hail#4914) Fixed a check in Table/MatrixTable initialization that scaled O(n^2) with the total number of fields.

### Bug fixes

 - (hail#4754)(hail#4799) Fixed optimizer assertion errors related to certain types of pipelines using ``group_rows_by``.
 - (hail#4888) Fixed assertion error in BlockMatrix.sum.
 - (hail#4871) Fixed possible error in locally sorting nested collections.
 - (hail#4889) Fixed break in compatibility with extremely old MatrixTable/Table files.
 - (hail#4527)(hail#4761) Fixed optimizer assertion error sometimes encountered with ``hl.split_multi[_hts]``.

-----

## 0.2.4: Beginning of history!

We didn't start manually curating information about user-facing changes until version 0.2.4.

The full commit history is available [here](https://github.com/hail-is/hail/commits/master).