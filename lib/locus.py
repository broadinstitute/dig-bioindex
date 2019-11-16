import abc
import dataclasses
import itertools
import re


@dataclasses.dataclass
class Locus(abc.ABC):
    """
    A location in the genome. Abstract. Must be either a SNPLocus or
    a RegionLocus.
    """
    chromosome: str

    @staticmethod
    def of_record(record, chromosome_col, start_col, stop_col=None):
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
    def overlaps(self, chromosome, start, stop):
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

    def overlaps(self, chromosome, start, stop):
        """
        True if this locus is overlapped by the region.
        """
        return self.chromosome == chromosome and start <= self.position < stop


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

    def overlaps(self, chromosome, start, stop):
        """
        True if this locus is overlapped by the region.
        """
        return self.chromosome == chromosome and stop > self.start and start < self.stop


def chromosomes():
    """
    Return an iterator of all chromosomes.
    """
    return itertools.chain(range(1, 23), ['X', 'Y', 'XY', 'M'])


def parse_chromosome(s):
    """
    Parse and normalize a chromosome string, which may be prefixed with 'chr'.
    """
    match = re.fullmatch(r'(?:chr)?(\d{1,2}|x|y|xy|m)', s, re.IGNORECASE)

    if not match:
        raise ValueError(f'Failed to match chromosome against {s}')

    return match.group(1).upper()


def parse_locus(s):
    """
    Parse a locus string and return the chromosome, start, stop.
    """
    match = re.fullmatch(r'(?:chr)?(\d{1,2}|x|y|xy|m):(\d+)(?:([+-])(\d+))?', s, re.IGNORECASE)

    if not match:
        raise ValueError(f'Failed to match locus against {s}')

    chromosome, start, adjust, end = match.groups()

    # parse the start position
    start = int(start)

    # if the adjustment is a + then end is a length, otherwise a position
    if adjust == '+':
        end = start + int(end)
    else:
        end = int(end) if end else start + 1

    # stop position must be > start
    if end <= start:
        raise ValueError(f'Stop ({end}) must be > start ({start})')

    return chromosome.upper(), start, end


def parse_locus_columns(s):
    """
    Parse a locus string and return the chromosome, start, and stop columns.
    """
    match = re.fullmatch(r'([^:]+):([^-]+)(?:-(.+))?', s)

    if not match:
        raise ValueError(f'Failed to parse locus column names against {s}')

    return match.groups()
