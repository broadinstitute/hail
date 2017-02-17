# Hail

[![Gitter](https://badges.gitter.im/hail-is/hail.svg)](https://gitter.im/hail-is/hail?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge) [![CI Status](https://ci.hail.is/app/rest/builds/buildType:(id:HailSourceCode_HailCi)/statusIcon)](https://ci.hail.is/viewType.html?buildTypeId=HailSourceCode_HailCi&branch_HailSourceCode_HailMainline=%3Cdefault%3E&tab=buildTypeStatusDiv)

[Hail](https://hail.is) is an open-source, scalable framework for exploring and analyzing genetic data. Starting from sequencing or microarray data in [VCF](https://samtools.github.io/hts-specs/VCFv4.2.pdf) and [other formats](https://hail.is/reference.html#Importing), Hail can, for example:

 - generate variant annotations like call rate, Hardy-Weinberg equilibrium p-value, and population-specific allele count
 - generate sample annotations like mean depth, imputed sex, and TiTv ratio
 - load variant and sample annotations from text tables, JSON, VCF, VEP, and locus interval files
 - generate new annotations from existing annotations and the genotypes, and use these to filter samples, variants, and genotypes
 - find Mendelian violations in trios, analyze genetic similarity between samples via the GRM and IBD matrix, and compute sample scores and variant loadings using PCA
 - perform association analyses using linear, logistic, and linear mixed regression, and estimate heritability

All this functionality is exposed through **[Python](https://www.python.org/)** and backed by distributed algorithms built on top of **[Apache Spark](http://spark.apache.org/)** to efficiently analyze gigabyte-scale data on a laptop or terabyte-scale data on an on-prem cluster or in the cloud.

Hail is used in [published research](http://biorxiv.org/content/early/2016/06/06/050195) and as the core analysis platform of large-scale genomics efforts including [ExAC v2](http://exac.broadinstitute.org/) and [gnomAD](http://gnomad.broadinstitute.org/). The project began in Fall 2015 and is under very active development as we work toward a stable release, so we do not guarantee forward compatibility of formats and interfaces. Want to get involved in development? Check out the [Github repo](https://github.com/hail-is/hail), chat with us in the [Gitter dev room](https://gitter.im/hail-is/hail-dev), and view our keynote at [Spark Summit East 2017](https://spark-summit.org/east-2017/).

[![Hail keynote at Spark Summit East 2017](www/hail_spark_summit.png)](https://www.youtube.com/watch?v=6WoPZ3_nIXc)

#### Getting Started

To get started using Hail:

- follow the installation insturctions in [Getting Started](https://hail.is/hail/getting_started.html) 
- try the [Tutorial](https://hail.is/hail/tutorial.html), read the [Overview](https://hail.is/hail/overview.html), and explore the [Python API](https://hail.is/hail/index.html)
- chat with the Hail team in the [Hail Gitter](https://gitter.im/hail-is/hail) room

We encourage use of the [Discussion Forum](http://discuss.hail.is) for user and dev support, feature requests, and sharing your Hail-powered science. Please report any suspected bugs to [github issues](https://github.com/hail-is/hail/issues).

#### Hail Team

The Hail team is based in the [Neale lab](https://nealelab.squarespace.com/) at the [Stanley Center for Psychiatric Research](http://www.broadinstitute.org/scientific-community/science/programs/psychiatric-disease/stanley-center-psychiatric-research/stanle) of the [Broad Institute of MIT and Harvard](http://www.broadinstitute.org) and the [Analytic and Translational Genetics Unit](https://www.atgu.mgh.harvard.edu/) of [Massachusetts General Hospital](http://www.massgeneral.org/).

Contact the Hail team at
<a href="mailto:hail@broadinstitute.org"><code>hail@broadinstitute.org</code></a>.

#### Citing Hail

If you use Hail for published work, please cite the software:

 - Hail, [https://github.com/hail-is/hail](https://github.com/hail-is/hail)

and either the forthcoming manuscript describing Hail (if possible):

 - Cotton Seed, Alex Bloemendal, Jonathan M Bloom, Jacqueline I Goldstein, Daniel King, Timothy Poterba, Benjamin M. Neale.  _Hail: An Open-Source Framework for Scalable Genetic Data Analysis_.  In preparation.

or the following paper which includes a brief introduction to Hail in the online methods:

 - Andrea Ganna, Giulio Genovese, et al. _Ultra-rare disruptive and damaging mutations influence educational attainment in the general population_.  [Nature Neuroscience](http://www.nature.com/neuro/journal/vaop/ncurrent/full/nn.4404.html)

And we'd love to hear about your work in the [Science category](http://discuss.hail.is/c/science) of the discussion forum!
