import functools
import graphql.utilities
import pandas as pd
import requests
import types
import urllib.parse


class Domain(types.SimpleNamespace):
    """
    A BioIndex portal domain, which filters phenotypes and datasets.
    """
    pass


class Phenotype(types.SimpleNamespace):
    """
    Trait names (IDs), descriptions, and data.
    """
    pass


class Dataset(types.SimpleNamespace):
    """
    Individual dataset statistics.
    """
    pass


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
        self.domain = None

    def _fetch_portal(self, req, q=None, cls=types.SimpleNamespace):
        """
        Perform a portal query.
        """
        if q:
            req += '?' + urllib.parse.urlencode({'q': q})

        # fetch the set of data
        data = requests.get(f'{self.portal}/{req}').json()['data']

        # convert to namespaces
        return [cls(**i) for i in data]

    @functools.cached_property
    def domains(self):
        """
        Get all available domains.
        """
        return self._fetch_portal('groups', cls=Domain)

    def phenotypes(self, domain=None):
        """
        Get all available phenotypes.
        """
        domain = domain or self.domain

        # lookup all the phenotypes by name
        items = self._fetch_portal('phenotypes', q=domain and domain.name, cls=Phenotype)

        # sort the phenotypes by pretty name
        return sorted(items, key=lambda x: x.description)

    def datasets(self, domain=None):
        """
        Get all available datasets.
        """
        return self._fetch_portal('datasets', q=domain or self.domain, cls=Dataset)


class GraphQLClient(RESTClient):
    """
    A client which uses the GraphQL end-point for making queries to
    the BioIndex and returning results.
    """

    def __init__(self, **kwargs):
        """
        Initialize and download the GraphQL schema.
        """
        super().__init__(**kwargs)

        # fetch the schema document and build the schema
        self.schema = graphql.utilities.build_schema(requests.get(f'{self.bio}/schema').text)

        # ensure the schema exists, is valid, and was built successfully
        assert self.schema, 'Failed to build GraphQL schema'

    def query(self, q):
        """
        Submit a GraphQL query and fetch the results. The return value is
        multiple DataFrames: one per input of the query and in the order of
        the inputs in the query.
        """
        resp = requests.post(f'{self.bio}/query', data=q)
        data = resp.json()['data']

        # values are in insertion order of the query
        return [pd.DataFrame(rs) for rs in data.values()]
