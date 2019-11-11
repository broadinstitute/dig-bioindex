import abc
import dataclasses
import re


@dataclasses.dataclass
class Locus(abc.ABC):
    """
    A location in the genome. Abstract. Must be either a SNPLocus or
    a RegionLocus.
    """
    chromosome: str

    @abc.abstractmethod
    def __str__(self):
        pass

    def __post_init__(self):
        """
        Validate the chromosome is valid, and normalize it.
        """
        self.chromosome = parse_chromosome(self.chromosome)


@dataclasses.dataclass(eq=True)
class SNPLocus(Locus):
    position: int

    def __str__(self):
        return '%s:%d' % (self.chromosome, self.position)

    def __hash__(self):
        """
        Hash this locus using the string representation of it.
        """
        return hash(str(self))


@dataclasses.dataclass(eq=True)
class RegionLocus(Locus):
    start: int
    stop: int

    def __str__(self):
        return '%s:%d-%d' % (self.chromosome, self.start, self.stop)

    def __hash__(self):
        """
        Hash this locus using the string representation of it.
        """
        return hash(str(self))


def parse_chromosome(s):
    """
    Parse and normalize a chromosome string, which may be prefixed with 'chr'.
    """
    match = re.fullmatch(r'(?:chr)?(\d{1,2}|x|y|xy|m)', s)

    if not match:
        raise ValueError('Failed to match chromosome against %s' % s)

    return match.group(1).upper()


def parse_locus(s):
    """
    Parse a locus string and return the chromosome, start, stop.
    """
    match = re.fullmatch(r'(?:chr)?(\d{1,2}|x|y|xy|m):(\d+)(?:-(\d+))?', s, re.IGNORECASE)

    if not match:
        raise ValueError('Failed to match locus against %s' % s)

    chromosome, start, end = match.groups()

    # parse the start position
    start = int(start)

    # default end to start+1 if not provided
    end = int(end) if end else start + 1

    return chromosome.upper(), start, end


def parse_locus_columns(s):
    """
    Parse a locus string and return the chromosome, start, and stop columns.
    """
    match = re.fullmatch(r'([^:]+):([^-]+)(?:-(.+))?', s)

    if not match:
        raise ValueError('Failed to parse locus column names against %s' % s)

    return match.groups()
