`<div class="cmdhead"></div>

<div class="description"></div>

<div class="synopsis"></div>

<div class="options"></div>

<div class="cmdsubsection">
### Notes:

Hail has infrastructure for representing multiallelic variants, but
most analytic methods only support analyzing data represented as
biallelics.  Therefore, the current recommendation is to split
multiallelics using the command `splitmulti` when importing a VCF.

Command line options:
 - `--propagate-gq` -- Propagate GQ instead of computing from PL.  Intended for use with the Michigan GotCloud calling pipeline which stores PLs but sets the GQ to the quality of the posterior probabilities.  This option is experimental and will be removed when Hail supports posterior probabilities (PP).
 - `keep-star-alleles` -- Keeps the `*` alleles as variants

Example `splitmulti` command:
```
$ hail importvcf /path/to/file.vcf splitmulti write -o /path/to/file.vds
```

Methods that don't support multiallelics will generate a error message
if applied to a dataset that has not been split.  The list of commands
that support multiallelics are:
 
 - `annotatesamples`
 - `count`
 - `exportvariants`
 - `exportvcf`
 - `filtersamples`
 - `filtervariants`
 - `grep`
 - `read`
 - `renamesamples`
 - `repartition`
 - `printschema`
 - `splitmulti`
 - `vep`
 - `write`
</div>

<div class="cmdsubsection">
### Implementation Details:

We will explain by example.  Consider a hypothetical 3-allelic variant
```
A	C,T	0/2:7,2,6:15:45:99,50,99,0,45,99
```

`splitmulti` will create two biallelic variants (one for each
alternate allele) at the same position:
```
A	C	0/0:13,2:15:45:0,45,99
A	T	0/1:9,6:15:50:50,0,99
```

Each multiallelic GT field is downcoded once for each alternate
allele.  A call for an alternate allele maps to 1 in the biallelic
variant corresponding to itself and 0 otherwise.  For example, in the
example above, 0/2 maps to 0/0 and 0/1.  The genotype 1/2 maps to 0/1
and 0/1.

The biallelic alt AD entry is just the multiallelic AD entry
corresponding to the alternate allele.  The ref AD entry is the sum of
the other multiallelic entries.

The biallelic DP is the same as the multiallelic DP.

The biallelic PL entry for for a genotype `g` is the minimum over PL
entries for multiallelic genotypes that downcode to `g`.  For example,
the PL for (A, T) at 0/1 is the minimum of the PLs for 0/1 (50) and
1/2 (45), and thus 45.

Fixing an alternate allele and biallelic variant, downcoding gives a
map from multiallelic to biallelic alleles and genotypes.  The
biallelic AD entry for an allele is just the sum of the multiallelic
AD entries for alleles that map to that allele.  Similarly, the
biallelic PL entry for a genotype is the minimum over multiallelic PL
entries for genotypes that map to that genotype.

By default, GQ is recomputed from PL.  If `--propagate-gq` is used,
the biallelic GQ field is simply the multiallelic GQ field, that is,
genotype qualities are unchanged.

Here is a second example for a het non-ref:
```
A	C,T	1/2:2,8,6:16:45:99,50,99,45,0,99
```
splits as
```
A	C	0/1:8,8:16:45:45,0,99
A	T	0/1:10,6:16:50:50,0,99
```
</div>


<div class="cmdsubsection">
### VCF Info Fields:

Hail does not split annotations in the info field.  This means that if a multiallelic site with info.AC value `[10, 2]` is split, each split site will contain the same array `[10, 2]`.  The provided allele index annotation `va.aIndex` can be used to select the value corresponding to the split allele's position:

```
$ hail importvcf 1kg.vcf.bgz
    splitmulti
    filtervariants expr -c 'va.info.AC[va.aIndex - 1] < 10' --remove
```

**VCFs split by Hail and exported to new VCFs may be incompatible with other tools, if action is not taken first.**  Since the "Number" of the arrays in split multiallelic sites no longer matches the structure on import ("A" for 1 per allele, for example), Hail will export these fields with number ".".

If the desired output is one value per site, then it is possible to use `annotatevariants expr` to remap these values.  Here is an example:

```
$ hail importvcf 1kg.vcf.bgz
    splitmulti 
    annotatevariants expr -c 'va.info.AC = va.info.AC[va.aIndex - 1]'
    exportvcf -o 1kg.split.vcf.bgz
```

After this pipeline, the info field "AC" in `1kg.split.vcf.bgz` will have number "1".
</div>

<div class="cmdsubsection">
### <a name="splitmulti_annotations"></a> Annotations:

 - `va.wasSplit : Boolean` -- this variant was originally multiallelic
 - `va.aIndex : Int` -- the original index of this alternate allele in the multiallelic representation (NB: 1 is the first alternate allele or the only alternate allele in a biallelic variant). For example, `1:100:A:T,C` yields two instances: `1:100:A:T` with aIndex = 1 and `1:100:A:C` with aIndex = 2.

</div>
