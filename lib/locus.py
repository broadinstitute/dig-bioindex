import abc
import itertools
import locale
import re
import requests


class Locus(abc.ABC):
    """
    A location in the genome. Abstract. Must be either a SNPLocus or
    a RegionLocus.
    """

    LOCUS_STEP = 20000

    def __init__(self, chromosome):
        """
        Ensure a valid chromosome.
        """
        self.chromosome = parse_chromosome(chromosome)

    @abc.abstractmethod
    def __str__(self):
        pass

    @abc.abstractmethod
    def loci(self):
        """
        A generator of record loci as tuples ('chromosome', position)
        """
        pass

    @abc.abstractmethod
    def overlaps(self, chromosome, start, stop):
        """
        True if this locus overlaps a region.
        """
        pass

    def stepped_pos(self, pos):
        """
        Returns a position as a stepped position.
        """
        return (pos // self.LOCUS_STEP) * self.LOCUS_STEP


class SNPLocus(Locus):
    """
    Locus for a single SNP (base pair) at an exact position.
    """

    def __init__(self, chromosome, position):
        super().__init__(chromosome)

        # ensure integer position
        self.position = int(position)

    def __str__(self):
        """
        Return a string representation of the locus.
        """
        return f'{self.chromosome}:{self.position}'

    def loci(self):
        """
        A generator of record loci. Reduce the total number of records by
        dividing and placing them in buckets.
        """
        yield self.chromosome, self.stepped_pos(self.position)

    def overlaps(self, chromosome, start, stop):
        """
        True if this locus is overlapped by the region.
        """
        return self.chromosome == chromosome and start <= self.position < stop


class RegionLocus(Locus):
    """
    Locus for a region on a chromosome.
    """

    def __init__(self, chromosome, start, stop):
        super().__init__(chromosome)

        # ensure integer range
        self.start = int(start)
        self.stop = int(stop)

    def __str__(self):
        """
        Return a string representation of the locus.
        """
        return f'{self.chromosome}:{self.start}-{self.stop}'

    def loci(self):
        """
        A generator of record loci.
        """
        start = self.start // self.LOCUS_STEP
        stop = self.stop // self.LOCUS_STEP

        for position in range(start, stop + 1):
            yield self.chromosome, position * self.LOCUS_STEP

    def overlaps(self, chromosome, start, stop):
        """
        True if this locus is overlapped by the region.
        """
        return self.chromosome == chromosome and stop > self.start and start < self.stop


def chromosomes():
    """
    Return an iterator of all chromosomes.
    """
    return itertools.chain(range(1, 23), ['X', 'Y', 'XY', 'M', 'MT'])


def parse_chromosome(s):
    """
    Parse and normalize a chromosome string, which may be prefixed with 'chr'.
    """
    match = re.fullmatch(r'(?:chr)?(\d{1,2}|x|y|xy|m|mt)', s, re.IGNORECASE)

    if not match:
        raise ValueError(f'Failed to match chromosome against {s}')

    return match.group(1).upper()


def parse_columns(s):
    """
    Parse a locus string and return the locus class, and column names
    as a tuple. If not a valid locus string, returns None and a tuple
    of all None.
    """
    match = re.fullmatch(r'([^:]+):([^-]+)(?:-(.+))?', s)

    if not match:
        return None, (None, None, None)

    cols = match.groups()

    # return the class and columns parsed
    return (RegionLocus if cols[2] else SNPLocus), cols


def parse(s, gene_lookup_engine=False):
    """
    Parse a locus string and return the chromosome, start, stop.
    """
    match = re.fullmatch(r'(?:chr)?(\d{1,2}|x|y|xy|mt):([\d,]+)(?:([+/-])([\d,]+))?', s, re.IGNORECASE)

    if not match:
        if not gene_lookup_engine:
            raise ValueError(f'Failed to match locus against {s}')
        return request_gene_locus(gene_lookup_engine, s)

    chromosome, start, adjust, end = match.groups()
    cur_locale = locale.getlocale()

    try:
        # parse thousands-separator commas
        locale.setlocale(locale.LC_ALL, 'en_US.UTF8')

        # parse the start position
        start = locale.atoi(start)

        # if the adjustment is a + then end is a length, otherwise a position
        if adjust == '+':
            end = start + locale.atoi(end)
        elif adjust == '/':
            shift = locale.atoi(end)
            start, end = start - shift, start + shift + 1
        else:
            end = locale.atoi(end) if end else start + 1

        # stop position must be > start
        if end <= start:
            raise ValueError(f'Stop ({end}) must be > start ({start})')

        return chromosome.upper(), start, end

    finally:
        # restore the original locale
        locale.setlocale(locale.LC_ALL, cur_locale)


def request_gene_locus(engine, q):
    """
    Use the __Genes table to lookup the region of a gene.
    """
    sql = 'SELECT `chromosome`, `start`, `end` FROM `__Genes` WHERE `name` = %s'
    gene = engine.execute(sql, q).fetchone()

    if gene:
        return gene[0], gene[1], gene[2]

    raise ValueError(f'Invalid locus or gene name: {q}')
