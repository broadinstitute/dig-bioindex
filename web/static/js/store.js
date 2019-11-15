let store = new Vuex.Store({
    state: {
        tab: null,

        // how many estimated records per key space
        metaAnalysisCount: 0,
        geneCount: 0,
        regionCount: 0,

        // records per key space
        metaAnalysis: [],
        genes: [],
        regions: [],
    },
    mutations: {
        setTab: (state, tab) => state.tab = tab,

        // estimated counts
        setMetaAnalysisCount: (state, n) => state.metaAnalysisCount = n,
        setGeneCount: (state, n) => state.geneCount = n,
        setRegionCount: (state, n) => state.regionCount = n,

        // fetched records
        setMetaAnalysis: (state, records) => state.metaAnalysis = records,
        setGenes: (state, records) => state.genes = records,
        setRegions: (state, records) => state.regions = records,
    },
    actions: {
        // simple guess as to the number of records in the region
        async countMetaAnalysis(context, { locus }) {
            await count(context, 'setMetaAnalysisCount', locus);
        },
        async countGenes(context, { locus }) {
            await count(context, 'setGeneCount', locus);
        },
        async countRegions(context, { locus }) {
            await count(context, 'setRegionCount', locus);
        },

        // perform fetch queries to get the actual records
        async queryMetaAnalysis(context, { locus }) {
            await query(context, 'setMetaAnalysis', 'metaanalysis', locus);
        },
        async queryGenes(context, { locus }) {
            await query(context, 'setGenes', 'genes', locus);
        },
        async queryRegions(context, { locus }) {
            await query(context, 'setRegions', 'regions', locus);
        }
    }
});

// request the estimated count for the number of records
function count({commit}, mutation, key, locus) {
    return fetch(`/api/count/${key}?q=${encodeURIComponent(locus)}`)
        .then(resp => resp.json())
        .then(json => commit(mutation, json.n))
        .catch(err => commit(mutation, 0));
}

// fetch all the records for a key in the given region
function query({commit}, mutation, key, locus) {
    commit('setTab', key);
    commit(mutation, undefined);

    return fetch(`/api/query/${key}?q=${encodeURIComponent(locus)}`)
        .then(resp => resp.json())
        .then(json => commit(mutation, json.records))
        .catch(err => commit(mutation, []));
}
