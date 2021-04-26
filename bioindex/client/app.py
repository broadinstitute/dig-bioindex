import graphql.utilities
import pandas as pd
import streamlit as st

from bioindex.client import GraphQLClient
from bioindex.client import plot


# opening documentation
st.write('''
# BioIndex Sample Notebook App

This is just an example client, notebook application the BioIndex GraphQLClient
class to fetch DataFrames and render tables/plots. To the side, you can set the
host address of the BioIndex to connect to and query from.

This app is built using [Streamlit](https://streamlit.io), but the client code
works just as well in [Jupyter](https://jupyter.org/) or outside a notebook-like
environment. The plots are all created with [Altair](https://altair-viz.github.io/).
''')

# enter the base URL of the BioIndex to references
base_url = st.sidebar.text_input('BioIndex URL', value='http://localhost:5000')
if not base_url:
    st.stop()

# create the client
client = GraphQLClient(base_url)

@st.cache(persist=True, show_spinner=False)
def fetch_domains():
    with st.spinner(text='Downloading domains...'):
        return client.domains.to_dict(orient='records')

@st.cache(persist=True, show_spinner=False)
def fetch_phenotypes(domain):
    client.domain = domain['name']
    with st.spinner(text='Downloading phenotypes...'):
        return client.phenotypes.to_dict(orient='records')


# select a domain
domain = st.sidebar.selectbox('Portal Domain', fetch_domains(), format_func=lambda i: i['description'])

# select one or more phenotypes
phenotypes = st.sidebar.multiselect('Phenotypes', fetch_phenotypes(domain), format_func=lambda i: i['description'])

# optionally select a region of the genome to limit results to
locus = st.sidebar.text_input('Region')

# let the user decide whether or not to view the GraphQL schema
if st.checkbox('Download and view GraphQL schema'):
    st.code(graphql.utilities.print_schema(client.schema), language='graphql')

# no phenotype or locus picked? show instructions and quit
if not phenotypes and not locus:
    st.write(
        '''
        ## Instructions
        This is an interactive notebook.

        On the side, you may select a portal domain, which will limit the results you
        see to only those phenotypes available in that domain.

        Once a domain is chosen, you may optionally pick one or more phenotypes as well
        as a region of the genome to limit your results to. The region - if entered -
        should be either a gene name or region string like "chr9:21,940,000-22,190,000".
        '''
    )

# ensure there's something to query
if not phenotypes and not locus:
    st.stop()

# this is the graphql query that we'll send to the client
q = []

# helper function that shows the query and runs it
def run_query():
    qs = '\n  '.join(q)
    qs = f'query {{\n  {qs}\n}}'

    # show the query in a code block
    st.subheader('GraphQL query executed')
    st.code(qs, language='graphql')

    # run it and return the frame(s)
    with st.spinner(text='Running query...'):
        return client.query(qs)

# just phenotypes... no locus, get some global data
if not locus:
    for phenotype in phenotypes:
        name = phenotype['name']

        # lookup the global data for these phenotypes
        q.append(f'{name}_Associations: GlobalAssociations(phenotype: "{name}") {{ varId, phenotype, pValue, beta }}')
        q.append(f'{name}_Enrichment: GlobalEnrichment(phenotype: "{name}") {{ phenotype, tissue, annotation, SNPs, expectedSNPs }}')

    # issue the query to the client
    frames = run_query()

    # join all the associations and enrichments together into a single frame
    assocs = pd.concat(frames[f'{p["name"]}_Associations'] for p in phenotypes)
    enrich = pd.concat(frames[f'{p["name"]}_Enrichment'] for p in phenotypes)

    # show a dataframe table of the associations, sorted by p-value
    st.subheader('Top single-variant association signals for selected phenotypes')
    st.dataframe(assocs.sort_values(by='pValue'))

    # calculate the fold and plot the enrichments
    st.subheader('Globally enriched tissues + annotations for selected phenotypes')
    enrich['fold'] = enrich['SNPs'] / enrich['expectedSNPs']
    st.altair_chart(plot.enrichment(enrich), use_container_width=True)

    # done, let's celebrate!
    st.balloons()
    st.stop()


# locus is present, so always show the genes overlapping and top associations
q.append(f'Genes(locus: "{locus}") {{ name, chromosome, start, end, source }}')
q.append(f'TopAssociations(locus: "{locus}") {{ varId, phenotype, pValue, beta }}')

# are there phenotypes? if so, download the associations for them
for phenotype in phenotypes:
    name = phenotype['name']
    q.append(f'{name}: Associations(phenotype: "{name}", locus: "{locus}") {{ varId, position, phenotype, pValue, beta }}')

# issue the query to the client
frames = run_query()

# show a table of all genes overlapping the region
st.subheader('Genes overlapping region')
df = frames['Genes'][frames['Genes']['source'] == 'symbol']
st.dataframe(df)

# show a phewas plot of all the top variant associations
st.subheader('Most significant variant associations in the region')
top = frames['TopAssociations'].sort_values(by='pValue').drop_duplicates(subset=['phenotype'])
st.altair_chart(plot.phewas(top), use_container_width=True)

# if phenotypes were present, plot the associations in the region
if phenotypes:
    st.subheader('Variants in the region associated with the selected phenotypes')
    df = pd.concat(frames[p['name']] for p in phenotypes)
    st.altair_chart(plot.associations(df), use_container_width=True)

# add a sidebar item for possible variants
variants = st.sidebar.multiselect('Variants', top['varId'].unique())

# show phewas per variant
if variants:
    q = [f'v{i}: PhewasAssociations(varId: "{v}") {{ varId, phenotype, pValue, beta }}' for i, v in enumerate(variants)]
    v_frames = run_query()

    # only keep those associations in the 50th-percentile for the variants
    st.subheader('Associations in the top 50th percentile across selected variants')
    df = pd.concat(d for d in v_frames.values())
    df = df[df.pValue <= min(0.05, df.pValue.min() / 0.5)]
    st.altair_chart(plot.phewas(df), use_container_width=True)

# done, let's celebrate!
st.balloons()
