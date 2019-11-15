Vue.use(BootstrapTable);

// entry point
let app = new Vue({
    el: '#app',
    store,
    components: {
        'BootstrapTable': BootstrapTable,
    },
    data: {
        locus: '',
        regionColumns: [
            { title: 'Region', formatter(v, r) { return `${r.chromosome}:${r.start}-${r.end}`; } },
            { title: 'Tissue', field: 'biosample' },
            { title: 'Method', field: 'method' },
            { title: 'Annotation', field: 'annotation' },
            { title: 'Gene', field: 'predictedTargetGene' },
            { title: 'Score', field: 'score' },
        ],
        geneColumns: [
            { title: 'Gene', field: 'name' },
            { title: 'ENS', field: 'ensemblId' },
            { title: 'Start', field: 'start' },
            { title: 'End', field: 'end' },
            { title: 'Type', field: 'type' },
        ],
    },
    computed: {
        tab() { return this.$store.state.tab },

        noMetaAnalysis() { return !this.$store.state.metaAnalysisCount },
        noGenes() { return !this.$store.state.geneCount },
        noRegions() { return !this.$store.state.regionCount },

        regionData() { return this.$store.state.regions },
        geneData() { return this.$store.state.genes },
    },
    watch: {
        locus: debounce(function (locus) {
            this.$store.dispatch('countMetaAnalysis', { locus });
            this.$store.dispatch('countGenes', { locus });
            this.$store.dispatch('countRegions', { locus });
        }, 500),
    }
});

// call function after a delay, overwrite any previous delay in place
function debounce(f, wait, immediate) {
    let timeout = null;

    return function () {
        let context = this;
        let args = arguments;
        let callNow = immediate && !timeout;

        let later = function () {
            timeout = null;
            if (!immediate) {
                f.apply(context, args);
            }
        };

        clearTimeout(timeout);
        timeout = setTimeout(later, wait);

        if (callNow) {
            f.apply(context, args);
        }
    };
}
