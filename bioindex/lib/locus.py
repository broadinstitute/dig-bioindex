import abc
import functools
import itertools
import re
import string


def _atoi(s):
    """Parse an integer with optional thousands-separator commas.

    Replaces a previous locale.atoi() call. setlocale is process-global, which
    is unsafe under the threaded server, and required a locale package the
    container image didn't ship.
    """
    return int(s.replace(',', ''))


class Locus(abc.ABC):
    """
    A location in the genome. Abstract. Must be either a SNPLocus or
    a RegionLocus.
    """

    LOCUS_STEP = 20000

    def __init__(self, chromosome, step=None):
        """
        Ensure a valid chromosome. ``step`` is the locus bucket size; None
        falls back to the class default LOCUS_STEP (20000) so existing
        callers are unchanged.
        """
        self.chromosome = parse_chromosome(chromosome)
        self.step = step if step is not None else self.LOCUS_STEP

    @abc.abstractmethod
    def __str__(self):
        pass

    @abc.abstractmethod
    def region(self):
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
        Returns a position as a stepped (bucketed) position.
        """
        return (pos // self.step) * self.step


class SNPLocus(Locus):
    """
    Locus for a single SNP (base pair) at an exact position.
    """

    def __init__(self, chromosome, position, step=None):
        super().__init__(chromosome, step=step)

        # ensure integer position
        self.position = int(position)

    def __str__(self):
        """
        Return a string representation of the locus.
        """
        return f'{self.chromosome}:{self.position}'

    def region(self):
        """
        Returns the complete range of this locus: [position,position+1).
        """
        return self.chromosome, self.position, self.position+1

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

    def __init__(self, chromosome, start, stop, step=None):
        super().__init__(chromosome, step=step)

        # ensure integer range
        self.start = int(start)
        self.stop = int(stop)

    def __str__(self):
        """
        Return a string representation of the locus.
        """
        return f'{self.chromosome}:{self.start}-{self.stop}'

    def region(self):
        """
        Returns the complete range of this locus: [start,stop).
        """
        return self.chromosome, self.start, self.stop

    def loci(self):
        """
        A generator of record loci, bucketed by self.step.
        """
        start = self.start // self.step
        stop = self.stop // self.step

        for position in range(start, stop + 1):
            yield self.chromosome, position * self.step

    def overlaps(self, chromosome, start, stop):
        """
        True if this locus is overlapped by the region.
        """
        return self.chromosome == chromosome and stop > self.start and start < self.stop


def chromosomes():
    """
    Return an iterator of all chromosomes.
    """
    return itertools.chain(range(1, 23), ['X', 'Y', 'XY', 'MT'])


def parse_chromosome(s):
    """
    Parse and normalize a chromosome, which may be prefixed with 'chr'.

    Accepts numeric input (e.g. an int from a numeric JSON column) by coercing
    to str before matching.
    """
    match = re.fullmatch(r'(?:chr)?([1-9]|1\d|2[0-2]|x|y|xy|mt?)', str(s), re.IGNORECASE)

    if not match:
        raise ValueError(f'Failed to match chromosome against {s}')

    return match.group(1).upper()


def parse_locus_builder(s, step=None):
    """
    Parse a locus string and return (builder, columns). The builder, when
    passed column values, returns a Locus with ``step`` baked in. Returns
    (None, None) if not a valid locus string.
    """
    match = re.fullmatch(r'([^=]+)=(.+)', s)

    # is this a field=template locus?
    if match:
        column, format_str = match.groups()

        fields = {
            'chr': r'(?P<chr>(?:chr)?(?:[1-9]|1\d|2[0-2]|x|y|xy|mt))',
            'pos': r'(?P<pos>[\d,]+)',
            'start': r'(?P<start>[\d,]+)',
            'stop': r'(?P<stop>[\d,]+)',
        }

        template = string.Template(f'^{format_str}').substitute(fields)
        pattern = re.compile(template, re.IGNORECASE)

        def build_locus(value):
            match = re.match(pattern, value)

            if not match:
                raise ValueError(f'Invalid locus: {value}')

            groups = match.groups()
            if len(groups) == 2:
                return SNPLocus(match.group('chr'), match.group('pos'), step=step)
            elif len(groups) == 3:
                return RegionLocus(match.group('chr'), match.group('start'), match.group('stop'), step=step)
            else:
                raise ValueError(f'Invalid locus: {value}')

        return build_locus, (column,)

    # extract the locus column names
    match = re.fullmatch(r'([^:]+):([^-]+)(?:-(.+))?', s)

    if not match:
        return None, None

    chromosome, start, stop = match.groups()

    # bake the step into the class via partial so Schema's call sites are unchanged
    cls = RegionLocus if stop else SNPLocus
    return functools.partial(cls, step=step), (chromosome, start, stop)


def parse_region_string(s, config):
    """
    Parse a locus string and return the chromosome, start, stop, and
    optional exact locus id.
    """
    match = re.fullmatch(r'(?:chr)?([1-9]|1\d|2[0-2]|x|y|xy|mt):([\d,]+)(?:([+/-])([\d,]+))?', s, re.IGNORECASE)

    if not match:
        region = config.genes_dict.get(s.upper())
        if not region:
            raise ValueError(f'Failed to parse locus "{s}" and no such gene')
        return region.chromosome.upper(), region.start, region.stop

    chromosome, start, adjust, end = match.groups()

    start = _atoi(start)

    # if the adjustment is a + then end is a length, otherwise a position
    if adjust == '+':
        end = start + _atoi(end)
    elif adjust == '/':
        shift = _atoi(end)
        start, end = start - shift, start + shift + 1
    else:
        end = _atoi(end) if end else start + 1

    # stop position must be > start
    if end <= start:
        raise ValueError(f'Stop ({end}) must be > start ({start})')

    return chromosome.upper(), start, end


def build_region_str(gene=None, chromosome=None, position=None, start=None, end=None):
    """
    Given optional keywords, build and return a region string.
    """
    if gene:
        return gene

    # if no gene is present, then the chromosome is required
    if not chromosome:
        raise ValueError('Missing chromosome parameter; gene or chromosome required')

    # single base position
    if position:
        if start or end:
            raise ValueError('Cannot specify both position and start/end in locus')

        return f'{chromosome}:{position}'

    # range position
    if not start or not end:
        raise ValueError('Either position or start and end must be specified')

    return f'{chromosome}:{start}-{end}'
