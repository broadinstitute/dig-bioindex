import abc
import dataclasses
import itertools
import locale
import re
import requests


# used for parsing integers with commas
locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')


@dataclasses.dataclass
class Locus(abc.ABC):
    """
    A location in the genome. Abstract. Must be either a SNPLocus or
    a RegionLocus.
    """
    chromosome: str

    @staticmethod
    def from_record(record, chromosome_col, start_col, stop_col=None):
        """
        Create either a SNPLocus or a RegionLocus for the record.
        """
        if not stop_col:
            return SNPLocus(record[chromosome_col], int(record[start_col]))

        return RegionLocus(record[chromosome_col], int(record[start_col]), int(record[stop_col]))

    @abc.abstractmethod
    def __str__(self):
        pass

    def __post_init__(self):
        """
        Validate the chromosome is valid, and normalize it.
        """
        if self.chromosome is None:
            raise KeyError('Missing chromosome column from record')

        self.chromosome = parse_chromosome(self.chromosome)

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

    @abc.abstractmethod
    def co_located(self, other):
        """
        True if the locus will always index with another locus.
        """
        pass


@dataclasses.dataclass(eq=True)
class SNPLocus(Locus):
    position: int

    def __post_init__(self):
        """
        Ensure the proper types for the locus.
        """
        super().__post_init__()

        if self.position is None:
            raise KeyError('Missing position column from record')

        # ensure integer position
        self.position = int(self.position)

    def __str__(self):
        """
        Return a string representation of the locus.
        """
        return f'{self.chromosome}:{self.position}'

    def __hash__(self):
        """
        Hash this locus using the string representation of it.
        """
        return hash(str(self))

    def loci(self):
        """
        A generator of record loci.
        """
        yield self.chromosome, self.position

    def overlaps(self, chromosome, start, stop):
        """
        True if this locus is overlapped by the region.
        """
        return self.chromosome == chromosome and start <= self.position < stop

    def co_located(self, other):
        """
        True if the locus will always index with another locus.
        """
        if not isinstance(other, SNPLocus):
            return False

        return other.chromosome == self.chromosome and other.position == self.position


@dataclasses.dataclass(eq=True)
class RegionLocus(Locus):
    start: int
    stop: int

    def __post_init__(self):
        """
        Ensure the proper types for the locus.
        """
        super().__post_init__()

        if self.start is None:
            raise KeyError('Missing start column from record')
        if self.stop is None:
            raise KeyError('Missing stop column from record')

        # ensure integer range
        self.start = int(self.start)
        self.stop = int(self.stop)

    def __str__(self):
        """
        Return a string representation of the locus.
        """
        return f'{self.chromosome}:{self.start}-{self.stop}'

    def __hash__(self):
        """
        Hash this locus using the string representation of it.
        """
        return hash(str(self))

    def loci(self):
        """
        A generator of record loci.
        """
        step = 20000
        start = self.start // step
        stop = self.stop // step

        for position in range(start, stop + 1):
            yield self.chromosome, position * step

    def overlaps(self, chromosome, start, stop):
        """
        True if this locus is overlapped by the region.
        """
        return self.chromosome == chromosome and stop > self.start and start < self.stop

    def co_located(self, other):
        """
        True if the locus will always index with another locus.
        """
        if not isinstance(other, RegionLocus):
            return False

        return other.chromosome == self.chromosome and other.start == self.start


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
    Parse a locus string and return the chromosome, start, and stop columns.
    """
    match = re.fullmatch(r'([^:]+):([^-]+)(?:-(.+))?', s)

    if not match:
        raise ValueError(f'Failed to parse locus column names against {s}')

    return match.groups()


def parse(s, allow_ens_lookup=False):
    """
    Parse a locus string and return the chromosome, start, stop.
    """
    match = re.fullmatch(r'(?:chr)?(\d{1,2}|x|y|xy|mt):([\d,]+)(?:([+/-])([\d,]+))?', s, re.IGNORECASE)

    if not match:
        if not allow_ens_lookup:
            raise ValueError(f'Failed to match locus against {s}')
        return request_ens_locus(s)

    chromosome, start, adjust, end = match.groups()

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


def request_ens_locus(q):
    """
    Use the Ensembl REST API to try and find a given locus that may be
    identified by name.
    """
    req = 'https://grch37.rest.ensembl.org/lookup'

    # lookup the gene by ENS ID or canonical name
    if q.upper().startswith('ENSG'):
        req += f'/id/{q}'
    else:
        req += f'/symbol/homo_sapiens/{q}'

    # make the request
    resp = requests.get(req, headers={'Content-Type': 'application/json'})

    # not found or otherwise invalid
    if resp.ok:
        body = resp.json()

        # fetch the chromosome, start, and stop positions
        chromosome = body.get('seq_region_name')
        start = body.get('start')
        stop = body.get('end')

        # must have a valid chromosome and start position
        if chromosome and start and stop:
            return chromosome, start, stop

    raise ValueError(f'Invalid locus or gene name: {q}')
