import functools
import graphql.utilities
import itertools
import pandas as pd
import re
import requests
import urllib.parse


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
        self.bio = f'{self.base_url}/api/bio'
        self.portal = f'{self.base_url}/api/portal'

        # the currently selected domain filter
        self._domain = None

    def _fetch_portal(self, req, q=None):
        """
        Perform a portal query.
        """
        if q:
            req += '?' + urllib.parse.urlencode({'q': q})

        # fetch the set of data
        data = requests.get(f'{self.portal}/{req}').json()['data']

        # build a dataframe
        return pd.DataFrame(data)

    @functools.cached_property
    def domains(self):
        """
        Get all available domains.
        """
        return self._fetch_portal('groups')

    @functools.cached_property
    def phenotypes(self):
        """
        Get all available phenotypes.
        """
        return self._fetch_portal('phenotypes', q=self._domain and self._domain['name'])

    @functools.cached_property
    def datasets(self):
        """
        Get all available datasets.
        """
        return self._fetch_portal('datasets', q=self._domain and self._domain['name'])

    def clear_cache(self):
        """
        Remove cached dataframes for phenotypes and datasets. This is
        done automatically whenever the selected domain changes.
        """
        if hasattr(self, 'phenotypes'):
            del self.phenotypes
        if hasattr(self, 'datasets'):
            del self.datasets

    @property
    def domain(self):
        """
        Returns the dictionary representing the selected domain.
        """
        return self._domain

    @domain.setter
    def domain(self, name):
        """
        Sets the selected domain, choosing it by name. To clear the selected
        domain, use the deleter property.
        """
        self._domain = self.domains.set_index('name', drop=False).loc[name].to_dict()
        self.clear_cache()

    @domain.deleter
    def domain(self):
        """
        Clear the selected domain.
        """
        self._domain = None
        self.clear_cache()

    def merge_phenotypes(self, df, on='phenotype', **merge_kwargs):
        """
        Merge and return a new DataFrame with known phenotypes.
        """
        df = df.merge(self.phenotypes, left_on='phenotype', right_on='name', **merge_kwargs)
        df = df.drop('name', axis=1)

        return df


class GraphQLClient(RESTClient):
    """
    A client which uses the GraphQL end-point for making queries to
    the BioIndex and returning results.
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize and download the GraphQL schema.
        """
        super().__init__(*args, **kwargs)

        # query builder which is executed
        self.queries = []

    @functools.cached_property
    def schema(self):
        """
        Downloads the GraphQL schema for this BioIndex and caches it.
        """
        return graphql.utilities.build_schema(requests.get(f'{self.bio}/schema').text)

    def query(self, q, concat=False):
        """
        Submit a GraphQL query and fetch the results. Returns a dictionary of
        DataFrames, one for each query.
        """
        resp = requests.post(f'{self.bio}/query', data=q)

        # invalid requests should throw
        if resp.status_code != 200:
            raise RuntimeError(resp.json()['detail'])

        # concatenate all the results together into a single frame
        if concat:
            return pd.DataFrame(itertools.chain.from_iterable(resp.json()['data'].values()))

        # values are in insertion order of the query
        return {k: pd.DataFrame(rs) for k, rs in resp.json()['data'].items()}
