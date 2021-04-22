import altair as alt
import math


try:
    # use the altair_viewer module if it exists and we're in a notebook
    from IPython import get_ipython

    if get_ipython().__class__.__name__ == 'ZMQInteractiveShell':
        import altair_viewer

        # enable it for embedding inline
        alt.renderers.enable('altair_viewer', inline=True)
except ModuleNotFoundError:
    pass


def genes(df, name='name', start='start', end='end'):
    """
    Line plot of genes along an x-axis.
    """
    pass


def phewas(df, variant='varId', p='pValue', trait='phenotype'):
    """
    Returns an Altair plot for PheWAS associations and the selection
    source for the phenotypes.
    """
    df = df.sort_values(by=p, ascending=True)
    df = df.drop_duplicates(subset=[trait])

    # create a log10 column for the y-axis
    df['-log10(p)'] = df[p].map(lambda p: -math.log10(p))

    # create the plot
    chart = alt.Chart(df).mark_bar().encode(
        y='phenotype',
        x='-log10(p)',
        color=alt.Color('phenotype', legend=None),
        tooltip=[variant, trait, p],
    )

    if not variant:
        return chart

    return chart


def associations(df, p='pValue', pos='position', trait='phenotype', effect='beta', marker='varId'):
    """
    Scatter plot of associations for a set of phenotype.
    """
    df['-log10(p)'] = df[p].map(lambda p: -math.log10(p))

    # create the plot
    return alt.Chart(df).mark_circle(size=60).encode(
        x=alt.X(field=pos, type='quantitative', scale=alt.Scale(zero=False, nice=False)),
        y='-log10(p)',
        color=trait,
        tooltip=[i for i in [marker, trait, p, effect]],
    )


def enrichment(df, trait='phenotype', tissue='tissue', annotation='annotation', fold='fold'):
    """
    Bubble plot of tissue fold enrichments per trait.
    """
    df = df.sort_values(by=fold, ascending=False)
    df = df.drop_duplicates(subset=['phenotype', 'tissue', 'annotation'])

    return alt.Chart(df).mark_circle().encode(
        x=annotation,
        y=tissue,
        color=trait,
        size=fold,
        tooltip=[tissue, trait, annotation, fold]
    )
