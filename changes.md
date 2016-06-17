
 - Added expr function `fet` to calculate p-values using Fisher's Exact Test.  Invoke this with `fet(count1, count2, count3, count4)` [see docs for details](docs/HailExpressionLanguage.md)
____

 - The `count` module no longer counts genotypes by default.  Use option `--genotypes` to do this.
 - Changed sample id column header in several modules.  They are now all "Sample".
 - Added expr function 'hwe'.  Invoke this with `hwe(homref_count, het_count, homvar_count)` 

____

 - Added several new pieces of functionality to expr language, which can be viewed in the [docs here](docs/HailExpressionLanguage.md)
  
    - array slicing, python-style
    - dicts, python-style (maps keyed by string)
    - function `index`, which takes an `Array[Struct]` and converts it to a Dict.  If `global.genes` is an `Array[Struct]` with struct type `Struct {geneID: String, PLI: Double, ExAC_LOFs: Int}`, then the function `index(global.genes, geneID)` will return a `Dict[Struct]` where the value has struct type `Struct {PLI: Double, ExAC_LOFs: Int}` (the key was pulled out)
    - Array and struct constructors: `[1, 2]` will give you an `Array[Int]`, and `{"gene": "SCN1A", "PLI": 0.999, "ExAC_LOFs": 5}` will give you a `Struct`.
    - Added a way to declare values missing: `NA: Type`. For example, you could say `if (condition) 5 else NA: Int`
 
____
 
 - Support JSON in annotation import.  `importannotations` is now
   `importannotations table`.  Added `importannotations json`,
   `annotatevariants json` and `annotatesamples json`.

____

 - Fixed some bad error messages, now errors will write a full stacktrace in the `hail.log`

____

 - added `annotateglobal table` which reads a text file with a header and stores it as an `Array[Struct]`.  [see docs for details](docs/ImportAnnotations.md#GlobalTable)

 - renamed all `tsv` modules to `table`.  We support arbitrary delimiters, so the name should be more general

____

 - split `annotateglobal` into `annotateglobal expr` and `annotateglobal list`.  The latter can be used to load a text file as an `Array[String]` or `Set[String]` to the global annotations, which can then be used to do things like `filtervariants expr` on genes in a gene list.
 
 - exposed `global` annotations throughout the expr language.  With aggregators, this allows you to do things like filter samples more than 5 standard deviations from the mean of a metric or the gene list filtering above
 
 ____

 - added **aggregators**!  see what you can do with them [here!](docs/HailExpressionLanguage.md)  As a consequence, removed some fields computed by `variantqc` and `sampleqc` because these are easy to compute with aggregators.

 - added `annotateglobal`, which lets you create global annotations with expressions and aggregators.  See docs for details.
 
 - renamed `showannotations` to `printschema`, and added `showglobals` to print out the current global annotations as JSON.

 - filtervariants and filtersamples have been reworked. Much like the annotate commands, you will need to specify `filtersamples expr` or `filtervariants intervals`. See the updated [filtering documentation](https://github.com/broadinstitute/hail/blob/master/docs/Filtering.md) for details.

 - filtervariants now supports filtering on .variant_list with lines
   of the form contig:pos:ref:alt1,alt2,...altN.
   
 - added new functions in expr language on arrays/sets: `filter`, `map`, `min`, `max`, `sum`.
 
 - added new function in expr language `str(x)` which gives a string representation of a thing (comma-delimited for arrays which can be changed with `arr.mkString(delim)`, and produces json for structs)  

 - `isNotMissing` renamed to `isDefined`.  `isMissing` and
   `isDefined` are now single-argument functions and can be used
   with structs, e.g., `isMissing(x)` instead of `x.isMissing`.

 - Fixed precedence bug in parser.  You can now write `!g.isHet`
   instead of `!(g.isNet)`.

 - count: `nLocalSamples` is gone.  `nSamples` reports the number of
   samples currently in the dataset.
