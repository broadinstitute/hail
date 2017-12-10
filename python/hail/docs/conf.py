# -*- coding: utf-8 -*-
#
# Hail documentation build configuration file, created by
# sphinx-quickstart on Fri Nov  4 10:55:10 2016.
#
# This file is execfile()d with the current directory set to its
# containing dir.
#
# Note that not all possible configuration values are present in this
# autogenerated file.
#
# All configuration values have a default; values that are commented out
# serve to show the default.

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#

import os
import sys

sys.path.insert(0, os.path.abspath('./_ext'))
sys.path.insert(0, os.path.abspath('../../hail2'))

# sys.path.insert(0, os.path.abspath('.'))
#import sphinx_rtd_theme

# -- General configuration ------------------------------------------------

# If your documentation needs a minimal Sphinx version, state it here.
#
needs_sphinx = '1.5.4'

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.autodoc',
    'hail_doctest', # replaced 'sphinx.ext.doctest'; new version does not check output
    'sphinx.ext.mathjax',
    'sphinx.ext.viewcode',
    'sphinx.ext.autosummary',
    'nbsphinx',
    'IPython.sphinxext.ipython_console_highlighting', # https://github.com/spatialaudio/nbsphinx/issues/24#issuecomment-187172022 and https://github.com/ContinuumIO/anaconda-issues/issues/1430
    'sphinxcontrib.napoleon'
]

mathjax_path = 'https://cdnjs.cloudflare.com/ajax/libs/mathjax/2.7.1/MathJax.js?config=TeX-AMS-MML_HTMLorMML'

nbsphinx_timeout = 300
nbsphinx_allow_errors = False

if not tags.has('checktutorial'):
    nbsphinx_execute = 'never'

autosummary_generate = ['api.rst', 'genetics/index.rst', 'methods/index.rst', 'utils/index.rst', 'linalg/index.rst']
# autoclass_content = "both"
autodoc_default_flags = ['members', 'undoc-members']

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates', '_templates/_autosummary']

doctest_global_setup = """import os, shutil
from hail import *
from hail.stats import *

if not os.path.isdir("output/"):
    os.mkdir("output/")

files = ["sample.vds", "sample.qc.vds", "sample.filtered.vds", "data/ld_matrix"]
for f in files:
    if os.path.isdir(f):
        shutil.rmtree(f)

hc = HailContext(log="output/hail.log", quiet=True)

(hc.import_vcf('data/sample.vcf.bgz')
 .sample_variants(0.03)
 .annotate_variants_expr('va.useInKinship = pcoin(0.9), va.panel_maf = 0.1, va.anno1 = 5, va.anno2 = 0, va.consequence = "LOF", va.gene = "A", va.score = 5.0')
 .annotate_variants_expr('va.aIndex = 1') # as if split_multi was called
 .variant_qc()
 .sample_qc()
 .annotate_samples_expr('sa.isCase = true, sa.pheno.isCase = pcoin(0.5), sa.pheno.isFemale = pcoin(0.5), sa.pheno.age=rnorm(65, 10), sa.cov.PC1 = rnorm(0,1), sa.pheno.height = rnorm(70, 10), sa.cov1 = rnorm(0, 1), sa.cov2 = rnorm(0,1), sa.pheno.bloodPressure = rnorm(120,20), sa.pheno.cohortName = "cohort1"')
 .write('data/example.vds', overwrite=True))

(hc.import_vcf("data/sample.vcf.bgz")
 .sample_variants(0.015).annotate_variants_expr('va.anno1 = 5, va.toKeep1 = true, va.toKeep2 = false, va.toKeep3 = true')
 .split_multi()
 .write("data/example2.vds", overwrite=True))

(hc.import_vcf("data/sample.vcf.bgz")
 .write("data/example2.multi.generic.vds", overwrite=True))

(hc.import_vcf('data/sample.vcf.bgz')
 .split_multi()
 .variant_qc()
 .annotate_samples_table(hc.import_table('data/example_lmmreg.tsv', 'Sample', impute=True), root='sa')
 .annotate_variants_expr('va.useInKinship = va.qc.AF > 0.05')
 .write("data/example_lmmreg.vds", overwrite=True))

(hc.import_vcf('data/example_burden.vcf')
 .annotate_samples_table(hc.import_table('data/example_burden.tsv', 'Sample', impute=True), root='sa.burden')
 .annotate_variants_expr('va.weight = v.start.toFloat64()')
 .variant_qc()
 .annotate_variants_table(KeyTable.import_interval_list('data/genes.interval_list'), root='va.genes', product=True)
 .annotate_variants_table(KeyTable.import_interval_list('data/gene.interval_list'), root='va.gene', product=False)
 .write('data/example_burden.vds', overwrite=True))

vds = hc.read('data/example.vds')

multiallelic_generic_vds = hc.read('data/example2.multi.generic.vds')

vds.split_multi().ld_matrix().write("data/ld_matrix")

"""

doctest_global_cleanup = """import shutil, os

hc.stop()

if os.path.isdir("output/"):
    shutil.rmtree("output/")

files = ["sample.vds", "sample.qc.vds", "sample.filtered.vds", "data/ld_matrix"]
for f in files:
    if os.path.isdir(f):
        shutil.rmtree(f)

"""

# The suffix(es) of source filenames.
# You can specify multiple suffix as a list of string:
#
# source_suffix = ['.rst', '.md']
source_suffix = '.rst'

# The encoding of source files.
#
# source_encoding = 'utf-8-sig'

# The master toctree document.
master_doc = 'index'

# General information about the project.
project = u'Hail'
copyright = u'2016, Hail Team'
author = u'Hail Team'

# The version info for the project you're documenting, acts as replacement for
# |version| and |release|, also used in various other places throughout the
# built documents.
#
# The short X.Y version.
version = os.environ['HAIL_VERSION']
# The full version, including alpha/beta/rc tags.
release = os.environ['HAIL_RELEASE']

# The language for content autogenerated by Sphinx. Refer to documentation
# for a list of supported languages.
#
# This is also used if you do content translation via gettext catalogs.
# Usually you set "language" from the command line for these cases.
language = None

# There are two options for replacing |today|: either, you set today to some
# non-false value, then it is used:
#
# today = ''
#
# Else, today_fmt is used as the format for a strftime call.
#
# today_fmt = '%B %d, %Y'

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This patterns also effect to html_static_path and html_extra_path
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

# The reST default role (used for this markup: `text`) to use for all
# documents.
#
# default_role = None

# If true, '()' will be appended to :func: etc. cross-reference text.
#
# add_function_parentheses = True

# If true, the current module name will be prepended to all description
# unit titles (such as .. function::).
#
# add_module_names = True

# If true, sectionauthor and moduleauthor directives will be shown in the
# output. They are ignored by default.
#
# show_authors = False

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = 'sphinx'

# A list of ignored prefixes for module index sorting.
# modindex_common_prefix = []

# If true, keep warnings as "system message" paragraphs in the built documents.
# keep_warnings = False

# If true, `todo` and `todoList` produce output, else they produce nothing.
todo_include_todos = False


# -- Options for HTML output ----------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
#html_theme = 'alabaster'
#html_theme = 'basic'
html_theme = 'sphinx_rtd_theme'
# Theme options are theme-specific and customize the look and feel of a theme
# further.  For a list of options available for each theme, see the
# documentation.
#
#html_theme_options = {
#}
html_theme_options = {
    'collapse_navigation': True,
    'display_version': True
}

# Add any paths that contain custom themes here, relative to this directory.
#html_theme_path = [sphinx_rtd_theme.get_html_theme_path()]
html_theme_path = ["_themes",]

# The name for this set of Sphinx documents.
# "<project> v<release> documentation" by default.
#
html_title = u'Hail'

# A shorter title for the navigation bar.  Default is the same as html_title.
#
# html_short_title = None

# The name of an image file (relative to this directory) to place at the top
# of the sidebar.
#
# html_logo = None

# The name of an image file (relative to this directory) to use as a favicon of
# the docs.  This file should be a Windows icon file (.ico) being 16x16 or 32x32
# pixels large.
#
html_favicon = "misc/hail_logo_sq.ico"

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']

# Add any extra paths that contain custom files (such as robots.txt or
# .htaccess) here, relative to this directory. These files are copied
# directly to the root of the documentation.
#
html_extra_path = ['../../../../../www/hail-logo-cropped.png',
                   '../../../../../www/navbar.css',
                   'misc/']

# If not None, a 'Last updated on:' timestamp is inserted at every page
# bottom, using the given strftime format.
# The empty string is equivalent to '%b %d, %Y'.
#
# html_last_updated_fmt = None

# If true, SmartyPants will be used to convert quotes and dashes to
# typographically correct entities.
#
# html_use_smartypants = True

# Custom sidebar templates, maps document names to template names.
#
html_sidebars = {
    '**': [ 'globaltoc.html', 'localtoc.html', 'searchbox.html']
}

# Additional templates that should be rendered to pages, maps page names to
# template names.
#
# html_additional_pages = {}

# If false, no module index is generated.
#
# html_domain_indices = True

# If false, no index is generated.
#
# html_use_index = True

# If true, the index is split into individual pages for each letter.
#
# html_split_index = False

# If true, links to the reST sources are added to the pages.
#
# html_show_sourcelink = True

# If true, "Created using Sphinx" is shown in the HTML footer. Default is True.
#
# html_show_sphinx = True

# If true, "(C) Copyright ..." is shown in the HTML footer. Default is True.
#
# html_show_copyright = True

# If true, an OpenSearch description file will be output, and all pages will
# contain a <link> tag referring to it.  The value of this option must be the
# base URL from which the finished HTML is served.
#
# html_use_opensearch = ''

# This is the file name suffix for HTML files (e.g. ".xhtml").
# html_file_suffix = None

# Language to be used for generating the HTML full-text search index.
# Sphinx supports the following languages:
#   'da', 'de', 'en', 'es', 'fi', 'fr', 'hu', 'it', 'ja'
#   'nl', 'no', 'pt', 'ro', 'ru', 'sv', 'tr', 'zh'
#
# html_search_language = 'en'

# A dictionary with options for the search language support, empty by default.
# 'ja' uses this config value.
# 'zh' user can custom change `jieba` dictionary path.
#
# html_search_options = {'type': 'default'}

# The name of a javascript file (relative to the configuration directory) that
# implements a search results scorer. If empty, the default will be used.
#
# html_search_scorer = 'scorer.js'

# Output file base name for HTML help builder.
htmlhelp_basename = 'haildoc'

# -- Options for LaTeX output ---------------------------------------------

latex_elements = {
     # The paper size ('letterpaper' or 'a4paper').
     #
     # 'papersize': 'letterpaper',

     # The font size ('10pt', '11pt' or '12pt').
     #
     # 'pointsize': '10pt',

     # Additional stuff for the LaTeX preamble.
     #
     # 'preamble': '',

     # Latex figure (float) alignment
     #
     # 'figure_align': 'htbp',
}

# Grouping the document tree into LaTeX files. List of tuples
# (source start file, target name, title,
#  author, documentclass [howto, manual, or own class]).
latex_documents = [
    (master_doc, 'Hail.tex', u'Hail Documentation',
     u'Hail Team', 'manual'),
]

# The name of an image file (relative to this directory) to place at the top of
# the title page.
#
# latex_logo = None

# For "manual" documents, if this is true, then toplevel headings are parts,
# not chapters.
#
# latex_use_parts = False

# If true, show page references after internal links.
#
# latex_show_pagerefs = False

# If true, show URL addresses after external links.
#
# latex_show_urls = False

# Documents to append as an appendix to all manuals.
#
# latex_appendices = []

# It false, will not define \strong, \code, 	itleref, \crossref ... but only
# \sphinxstrong, ..., \sphinxtitleref, ... To help avoid clash with user added
# packages.
#
# latex_keep_old_macro_names = True

# If false, no module index is generated.
#
# latex_domain_indices = True


# -- Options for manual page output ---------------------------------------

# One entry per manual page. List of tuples
# (source start file, name, description, authors, manual section).
man_pages = [
    (master_doc, 'hail', u'Hail Documentation',
     [author], 1)
]

# If true, show URL addresses after external links.
#
# man_show_urls = False


# -- Options for Texinfo output -------------------------------------------

# Grouping the document tree into Texinfo files. List of tuples
# (source start file, target name, title, author,
#  dir menu entry, description, category)
texinfo_documents = [
    (master_doc, 'Hail', u'Hail Documentation',
     author, 'Hail', 'One line description of project.',
     'Miscellaneous'),
]

# Documents to append as an appendix to all manuals.
#
# texinfo_appendices = []

# If false, no module index is generated.
#
# texinfo_domain_indices = True

# How to display URL addresses: 'footnote', 'no', or 'inline'.
#
# texinfo_show_urls = 'footnote'

# If true, do not generate a @detailmenu in the "Top" node's menu.
#
# texinfo_no_detailmenu = False
