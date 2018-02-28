from hail.typecheck import *
from hail.utils import wrap_to_list
from hail.utils.java import jiterable_to_list, Env
from hail.typecheck import oneof, transformed
import hail as hl


class ReferenceGenome(object):
    """An object that represents a `reference genome <https://en.wikipedia.org/wiki/Reference_genome>`__.

    Examples
    --------

    >>> contigs = ["1", "X", "Y", "MT"]
    >>> lengths = {"1": 249250621, "X": 155270560, "Y": 59373566, "MT": 16569}
    >>> par = [("X", 60001, 2699521)]
    >>> my_ref = hl.ReferenceGenome("my_ref", contigs, lengths, "X", "Y", "MT", par)

    Parameters
    ----------
    name : :obj:`str`
        Name of reference. Must be unique and NOT one of Hail's
        predefined references: ``'GRCh37'``, ``'GRCh38'``, and ``'default'``.
    contigs : :obj:`list` of :obj:`str`
        Contig names.
    lengths : :obj:`dict` of :obj:`str` to :obj:`int`
        Dict of contig names to contig lengths.
    x_contigs : :obj:`str` or :obj:`list` of :obj:`str`
        Contigs to be treated as X chromosomes.
    y_contigs : :obj:`str` or :obj:`list` of :obj:`str`
        Contigs to be treated as Y chromosomes.
    mt_contigs : :obj:`str` or :obj:`list` of :obj:`str`
        Contigs to be treated as mitochondrial DNA.
    par : :obj:`list` of :obj:`tuple` of (str, int, int)
        List of tuples with (contig, start, end)
    """

    _references = {}

    @typecheck_method(name=str,
                      contigs=listof(str),
                      lengths=dictof(str, int),
                      x_contigs=oneof(str, listof(str)),
                      y_contigs=oneof(str, listof(str)),
                      mt_contigs=oneof(str, listof(str)),
                      par=listof(sized_tupleof(str, int, int)))
    def __init__(self, name, contigs, lengths, x_contigs=[], y_contigs=[], mt_contigs=[], par=[]):
        contigs = wrap_to_list(contigs)
        x_contigs = wrap_to_list(x_contigs)
        y_contigs = wrap_to_list(y_contigs)
        mt_contigs = wrap_to_list(mt_contigs)

        par_strings = ["{}:{}-{}".format(contig, start, end) for (contig, start, end) in par]

        jrep = (Env.hail().variant.ReferenceGenome
                .apply(name,
                       contigs,
                       lengths,
                       x_contigs,
                       y_contigs,
                       mt_contigs,
                       par_strings))

        self._init_from_java(jrep)
        self._name = name
        self._contigs = contigs
        self._lengths = lengths
        self._x_contigs = x_contigs
        self._y_contigs = y_contigs
        self._mt_contigs = mt_contigs
        self._par = None
        self._par_tuple = par

        super(ReferenceGenome, self).__init__()
        ReferenceGenome._references[name] = self

    def __str__(self):
        return self._jrep.toString()

    def __repr__(self):
        if not self._par_tuple:
            self._par_tuple = [(x.start.contig, x.start.position, x.end.position) for x in self.par]
        return 'ReferenceGenome(name=%s, contigs=%s, lengths=%s, x_contigs=%s, y_contigs=%s, mt_contigs=%s, par=%s)' % \
               (self.name, self.contigs, self.lengths, self.x_contigs, self.y_contigs, self.mt_contigs, self._par_tuple)

    def __eq__(self, other):
        return isinstance(other, ReferenceGenome) and self._jrep.equals(other._jrep)

    def __hash__(self):
        return self._jrep.hashCode()

    @property
    def name(self):
        """Name of reference genome.

        Returns
        -------
        :obj:`str`
        """
        if self._name is None:
            self._name = self._jrep.name()
        return self._name

    @property
    def contigs(self):
        """Contig names.

        Returns
        -------
        :obj:`list` of :obj:`str`
        """
        if self._contigs is None:
            self._contigs = [str(x) for x in self._jrep.contigs()]
        return self._contigs

    @property
    def lengths(self):
        """Dict of contig name to contig length.

        Returns
        -------
        :obj:`list` of :obj:`str`
        """
        if self._lengths is None:
            self._lengths = {str(x._1()): int(x._2()) for x in jiterable_to_list(self._jrep.lengths())}
        return self._lengths

    @property
    def x_contigs(self):
        """X contigs.

        Returns
        -------
        :obj:`list` of :obj:`str`
        """
        if self._x_contigs is None:
            self._x_contigs = [str(x) for x in jiterable_to_list(self._jrep.xContigs())]
        return self._x_contigs

    @property
    def y_contigs(self):
        """Y contigs.

        Returns
        -------
        :obj:`list` of :obj:`str`
        """
        if self._y_contigs is None:
            self._y_contigs = [str(x) for x in jiterable_to_list(self._jrep.yContigs())]
        return self._y_contigs

    @property
    def mt_contigs(self):
        """Mitochondrial contigs.

        Returns
        -------
        :obj:`list` of :obj:`str`
        """
        if self._mt_contigs is None:
            self._mt_contigs = [str(x) for x in jiterable_to_list(self._jrep.mtContigs())]
        return self._mt_contigs

    @property
    def par(self):
        """Pseudoautosomal regions.

        Returns
        -------
        :obj:`list` of :class:`.Interval`
        """

        from hail.utils.interval import Interval
        if self._par is None:
            self._par = [Interval._from_java(jrep, hl.tlocus(self)) for jrep in self._jrep.par()]
        return self._par

    @typecheck_method(contig=str)
    def contig_length(self, contig):
        """Contig length.

        Parameters
        ----------
        contig : :obj:`str`
            Contig name.

        Returns
        -------
        :obj:`int`
            Length of contig.
        """
        if contig in self.lengths:
            return self.lengths[contig]
        else:
            raise KeyError("Contig `{}' is not in reference genome.".format(contig))

    @classmethod
    @typecheck_method(path=str)
    def read(cls, path):
        """Load reference genome from a JSON file.

        Notes
        -----

        The JSON file must have the following format:

        .. code-block:: text

            {"name": "my_reference_genome",
             "contigs": [{"name": "1", "length": 10000000},
                         {"name": "2", "length": 20000000},
                         {"name": "X", "length": 19856300},
                         {"name": "Y", "length": 78140000},
                         {"name": "MT", "length": 532}],
             "xContigs": ["X"],
             "yContigs": ["Y"],
             "mtContigs": ["MT"],
             "par": [{"start": {"contig": "X","position": 60001},"end": {"contig": "X","position": 2699521}},
                     {"start": {"contig": "Y","position": 10001},"end": {"contig": "Y","position": 2649521}}]
            }


        `name` must be unique and not overlap with Hail's pre-instantiated
        references: ``'GRCh37'``, ``'GRCh38'``, and ``'default'``.The contig
        names in `xContigs`, `yContigs`, and `mtContigs` must be present in
        `contigs`. The intervals listed in `par` must have contigs in either
        `xContigs` or `yContigs` and must have positions between 0 and the
        contig length given in `contigs`.

        Parameters
        ----------
        path : :obj:`str`
            Path to JSON file.

        Returns
        -------
        :class:`.ReferenceGenome`
        """
        return ReferenceGenome._from_java(Env.hail().variant.ReferenceGenome.fromFile(Env.hc()._jhc, path))

    @typecheck_method(output=str)
    def write(self, output):
        """"Write this reference genome to a file in JSON format.

        Examples
        --------

        >>> my_rg = hl.ReferenceGenome("new_reference", ["x", "y", "z"], {"x": 500, "y": 300, "z": 200})
        >>> my_rg.write("output/new_reference.json")

        Notes
        -----

        Use :class:`~hail.ReferenceGenome.read` to reimport the exported
        reference genome in a new HailContext session.

        Parameters
        ----------
        output : :obj:`str`
            Path of JSON file to write.
        """

        self._jrep.write(Env.hc()._jhc, output)

    def _init_from_java(self, jrep):
        self._jrep = jrep

    @classmethod
    def _from_java(cls, jrep):
        gr = ReferenceGenome.__new__(cls)
        gr._init_from_java(jrep)
        gr._name = None
        gr._contigs = None
        gr._lengths = None
        gr._x_contigs = None
        gr._y_contigs = None
        gr._mt_contigs = None
        gr._par = None
        gr._par_tuple = None
        super(ReferenceGenome, gr).__init__()
        ReferenceGenome._references[gr.name] = gr
        return gr

    def _check_locus(self, l_jrep):
        self._jrep.checkLocus(l_jrep)

    def _check_interval(self, interval_jrep):
        self._jrep.checkInterval(interval_jrep)

reference_genome_type = oneof(transformed((str, lambda x: hl.get_reference(x))), ReferenceGenome)
