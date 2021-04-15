import concurrent.futures
import dataclasses
import functools
import pandas as pd
import requests
import types
import urllib.parse

from bioindex.lib import locus


@dataclasses.dataclass
class Region:
    """
    Parsing abitratry regions or looking up regions by gene name.
    """

    chromosome: str
    start: int
    end: int
    gene: str=None

    @staticmethod
    def parse(s, client=None):
        try:
            c, s, e = locus.parse_region_string(s)

            # create the region from the locus
            return Region(chromosome=c, start=s, end=e)
        except ValueError:
            if client:
                return client.gene(s)

    def __str__(self):
        """
        Little helper function for query region strings.
        """
        if self.gene:
            return self.gene

        return f'{self.chromosome}:{self.start}-{self.stop}'


class RESTClient:
    """
    A REST API client for a BioIndex server.
    """

    def __init__(self, base_url='https://bioindex.hugeamp.org'):
        """
        Initialize the client. The base_url should be the IP/domain for
        BioIndex REST server running.
        """
        self.base_url = base_url.rstrip('/')

        # urls for all calls
        self.api = f'{self.base_url}/api'
        self.bio = f'{self.api}/bio'
        self.portal = f'{self.api}/portal'

        # the default domain
        self.domain = None

        # reusable query frame
        self.qf = QueryFrame(self.query)

    def _fetch_portal(self, req, q=None):
        """
        Perform a portal query.
        """
        if q:
            req += '?' + urllib.parse.urlencode({'q': q})

        # fetch the set of data
        data = requests.get(f'{self.portal}/{req}').json()['data']

        # convert to namespaces
        return [types.SimpleNamespace(**i) for i in data]

    @functools.cached_property
    def domains(self):
        """
        Get all available domains.
        """
        return self._fetch_portal('groups')

    @functools.cached_property
    def indexes(self):
        """
        Get all available indexes.
        """
        items = self._fetch_portal('indexes')

        # return the indexes, sorted by name
        return sorted(items, key=lambda x: x.name)

    def phenotypes(self, domain=None):
        """
        Get all available phenotypes.
        """
        domain = domain or self.domain

        # lookup all the phenotypes by name
        items = self._fetch_portal('phenotypes', q=domain and domain.name)

        # sort the phenotypes by pretty name
        return sorted(items, key=lambda x: x.description)

    def datasets(self, domain=None):
        """
        Get all available datasets.
        """
        return self._fetch_portal('datasets', q=domain or self.domain)

    def query(self, index, *q, limit=None):
        """
        Perform a basic BioIndex query. Returns a pandas DataFrame of
        the results.
        """
        records = []

        # build request url
        req = f'{self.bio}/query/{index}?q={",".join(q)}'

        # add the optional result count limit
        if isinstance(limit, int):
            req += f'&limit={limit}'

        # initial request
        resp = requests.get(req).json()
        cont = resp.get('continuation')

        # follow continuations
        while cont:
            records += resp['data']

            # get next request
            resp = requests.get(f'{BIO}/cont?token={cont}').json()
            cont = resp.get('continuation')

        # dataframe of records
        return pd.DataFrame(records + resp['data'])

    def gene(self, name):
        """
        Lookup the region given a gene.
        """
        df = self.query('gene', name)

        if df.empty:
            return None

        return Region(
            gene=df['name'][0],
            chromosome=df['chromosome'][0],
            start=df['start'][0],
            end=df['end'][0]
        )

    def genes(self, region):
        """
        Lookup all genes overlapping a region.
        """
        df = self.query('genes', str(region))

        # only return HGNC canonical gene symbols
        return df[df['source'] == 'symbol']

    def filter_phenotypes(self, df, domain=None, datasets=None):
        """
        Filter records by phenotypes visible to the domain and/or a
        list of datasets.
        """
        visible_phenotypes = [p.name for p in self.phenotypes(domain)]

        # filter the visible phenotypes to those in the datasets
        if datasets:
            visible_phenotypes = list(set(p for p in d.phenotypes for d in datasets))

        # filter the records
        return df[df['phenotype'].isin(visible_phenotypes)]

    def lookup_variants(self, variants):
        """
        Given a series or iterable of variants/dbSNPs, look them up and return
        the common results associated with them.
        """
        for v in variants:
            self.qf.submit('variant', v)

        return self.qf.dataframe

    def bottom_line(self, variants=None, traits=None, region=None):
        """
        Fetch the bottom-line results for a given variant, trait, and/or a region.
        """
        if not variants and not traits and not region:
            return pd.DataFrame()

        # all associations for a single variant
        if variants:
            for v in self.lookup_variants(variants)['varId']:
                self.qf.submit('phewas-associations', v)

            # collect the results
            df = self.qf.dataframe

            # sort them all by p-value, keep only the best per variant/trait
            df = df.sort_values(by='pValue', ascending=True)
            df = df.drop_duplicates(subset=['varId', 'phenotype'])

            # filter for just the traits if provided
            return df[df['phenotype'].isin(traits)] if traits else df

        # clumped, global results for a trait or limited to a region
        for trait in traits:
            if isinstance(trait, types.SimpleNamespace):
                trait = trait.name

            # genome-wide for the trait or limited to specific regions?
            if region:
                self.qf.submit('associations', trait, str(region))
            else:
                self.qf.submit('global-associations', trait)

        # just collect the top associations in regions
        if not traits:
            self.qf.submit('top-associations', str(region))

        # collect the results
        df = self.qf.dataframe
        df = df.sort_values(by='pValue', ascending=True)

        return df

    def enrichments(self, traits):
        """
        """
        for trait in traits:
            if isinstance(trait, types.SimpleNamespace):
                trait = trait.name
            self.qf.submit('global-enrichment', trait)

        # calculate the fold for each
        df = self.qf.dataframe
        df['fold'] = df['SNPs'] / df['expectedSNPs']

        return df


class QueryFrame:
    """
    """

    def __init__(self, proc, max_workers=5):
        """
        Initialize with no data loaded.
        """
        self.pool = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        self.proc = proc
        self.jobs = []
        self.df = None

    @property
    def dataframe(self):
        """
        Waits for all jobs created to complete and sets the final frame.
        """
        if self.df is not None:
            return self.df

        # don't cache the result, but return empty frame
        if not self.jobs:
            return pd.DataFrame()

        # wait for jobs to complete
        concurrent.futures.wait(self.jobs)

        # join all the frames together and cache it
        self.df = pd.concat(job.result() for job in self.jobs)

        return self.df

    def submit(self, *args, **kwargs):
        """
        Submits a job to the query frame. The result can be accessed
        with the dataframe() method, which is cached. If the dataframe()
        has already been accessed, this will clear all the previous
        results and begin submitting new jobs.
        """
        if self.df is not None:
            self.df = None
            self.jobs.clear()

        # submit the job and start processing it
        self.jobs.append(self.pool.submit(self.proc, *args, **kwargs))
