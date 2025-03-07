#!/bin/bash

env_file="bioindex-cancer.env"

yes | python3 -m bioindex.main --env-file "${env_file}" create global-enrichment GlobalEnrichment bioindex/partitioned_heritability/annotation-tissue/ phenotype
yes | python3 -m bioindex.main --env-file "${env_file}" create top-associations TopAssociations bioindex/associations/top/trans-ethnic/ chromosome:clumpStart-clumpEnd
yes | python3 -m bioindex.main --env-file "${env_file}" create credible-sets CredibleSets bioindex/credible_sets/locus/trans-ethnic/ phenotype,chromosome:start-end
yes | python3 -m bioindex.main --env-file "${env_file}" create credible-sets CredibleSetsAncestry bioindex/credible_sets/locus/ancestry/ phenotype,ancestry,chromosome:start-end
yes | python3 -m bioindex.main --env-file "${env_file}" create credible-variants CredibleVariants bioindex/credible_sets/variants/trans-ethnic/ phenotype,credibleSetId
yes | python3 -m bioindex.main --env-file "${env_file}" create credible-variants CredibleVariantsAncestry bioindex/credible_sets/variants/ancestry/ phenotype,ancestry,credibleSetId
yes | python3 -m bioindex.main --env-file "${env_file}" create global-associations GlobalAssociations bioindex/associations/global/trans-ethnic/ phenotype
yes | python3 -m bioindex.main --env-file "${env_file}" create ancestry-global-associations GlobalAssociationsAncestry bioindex/associations/global/ancestry/ phenotype,ancestry
yes | python3 -m bioindex.main --env-file "${env_file}" create gene-finder GeneFinder bioindex/finder/gene/trans-ethnic/ phenotype
yes | python3 -m bioindex.main --env-file "${env_file}" create gene-finder GeneFinderAncestry bioindex/finder/gene/ancestry/ phenotype,ancestry
yes | python3 -m bioindex.main --env-file "${env_file}" create ancestry-top-associations TopAssociationsAncestry bioindex/associations/top/ancestry/ ancestry,chromosome:clumpStart-clumpEnd
yes | python3 -m bioindex.main --env-file "${env_file}" create genetic-correlation GeneticCorrelation bioindex/genetic-correlation/trans-ethnic/ phenotype
yes | python3 -m bioindex.main --env-file "${env_file}" create genetic-correlation GeneticCorrelationAncestry bioindex/genetic-correlation/ancestry/ phenotype,ancestry
yes | python3 -m bioindex.main --env-file "${env_file}" create pathway-associations PathwayAssociations bioindex/pathway_associations/trans-ethnic/ phenotype
yes | python3 -m bioindex.main --env-file "${env_file}" create pathway-associations PathwayAssociationsAncestry bioindex/pathway_associations/ancestry/ phenotype,ancestry
yes | python3 -m bioindex.main --env-file "${env_file}" create huge-phenotype HugePhenotype bioindex/huge/phenotype/ phenotype
yes | python3 -m bioindex.main --env-file "${env_file}" create partitioned-heritability PartitionedHeritability bioindex/partitioned_heritability/trans-ethnic/ phenotype
yes | python3 -m bioindex.main --env-file "${env_file}" create partitioned-heritability PartitionedHeritabilityAncestry bioindex/partitioned_heritability/ancestry/ phenotype,ancestry
yes | python3 -m bioindex.main --env-file "${env_file}" create clumped-variants ClumpedVariants bioindex/associations/clump/trans-ethnic/ phenotype,clump
yes | python3 -m bioindex.main --env-file "${env_file}" create ancestry-clumped-variants ClumpedVariantsAncestry bioindex/associations/clump/ancestry/ phenotype,clump
yes | python3 -m bioindex.main --env-file "${env_file}" create c2ct C2CTAncestry bioindex/credible_sets/c2ct/all/ancestry/ phenotype,ancestry
yes | python3 -m bioindex.main --env-file "${env_file}" create c2ct C2CTTransEthnic bioindex/credible_sets/c2ct/all/trans-ethnic/ phenotype
yes | python3 -m bioindex.main --env-file "${env_file}" create c2ct-annotation C2CTAnnotationAncestry bioindex/credible_sets/c2ct/annotation/ancestry/ phenotype,ancestry,annotation
yes | python3 -m bioindex.main --env-file "${env_file}" create c2ct-annotation C2CTAnnotation bioindex/credible_sets/c2ct/annotation/trans-ethnic/ phenotype,annotation
yes | python3 -m bioindex.main --env-file "${env_file}" create c2ct-tissue C2CTTissueAncestry bioindex/credible_sets/c2ct/tissue/ancestry/ phenotype,ancestry,annotation,tissue
yes | python3 -m bioindex.main --env-file "${env_file}" create c2ct-tissue C2CTTissue bioindex/credible_sets/c2ct/tissue/trans-ethnic/ phenotype,annotation,tissue
yes | python3 -m bioindex.main --env-file "${env_file}" create c2ct-biosample C2CTBiosampleAncestry bioindex/credible_sets/c2ct/biosample/ancestry/ phenotype,ancestry,annotation,tissue,biosample
yes | python3 -m bioindex.main --env-file "${env_file}" create c2ct-biosample C2CTBiosample bioindex/credible_sets/c2ct/biosample/trans-ethnic/ phenotype,annotation,tissue,biosample
yes | python3 -m bioindex.main --env-file "${env_file}" create c2ct-credible-set C2CTCredibleSetAncestry bioindex/credible_sets/c2ct/credible_set_id/ancestry/ phenotype,ancestry,credibleSetId
yes | python3 -m bioindex.main --env-file "${env_file}" create c2ct-credible-set C2CTCredibleSet bioindex/credible_sets/c2ct/credible_set_id/trans-ethnic/ phenotype,credibleSetId
