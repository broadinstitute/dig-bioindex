
python3 -m bioindex.main create associations Associations bioindex/ancestry-associations/phenotype/ phenotype,chromosome:position
python3 -m bioindex.main create global-associations GlobalAssociations bioindex/ancestry-associations/global/ phenotype
python3 -m bioindex.main create top-associations TopAssociations bioindex/ancestry-associations/top/ chromosome:clumpStart-clumpEnd
python3 -m bioindex.main create gene-associations GeneAssociations bioindex/gene_associations/gene/ancestry-specific/ gene
python3 -m bioindex.main create gene-finder GeneFinder finder/gene/ancestry-specific/ phenotype
python3 -m bioindex.main create partitioned-heritability PartitionedHeritability bioindex/partitioned_heritability/ancestry/ phenotype
python3 -m bioindex.main create partitioned-heritability-tissue PartitionedHeritabilityTissue bioindex/partitioned_heritability/tissue/ tissue
python3 -m bioindex.main create pathway-associations PathwayAssociations bioindex/pathway_associations/ancestry-specific/ phenotype
python3 -m bioindex.main create dataset-associations DatasetAssociations bioindex/associations/dataset/ dataset,phenotype
python3 -m bioindex.main create genetic-correlation GeneticCorrelation bioindex/genetic-correlation/ancestry-specific/ phenotype
python3 -m bioindex.main create global-enrichment GlobalEnrichment bioindex/partitioned_heritability/annotation-tissue/ phenotype


python3 -m bioindex.main index associations
python3 -m bioindex.main index global-associations
python3 -m bioindex.main index top-associations
python3 -m bioindex.main index gene-associations
python3 -m bioindex.main index gene-finder
python3 -m bioindex.main index partitioned-heritability
python3 -m bioindex.main index global-enrichment
python3 -m bioindex.main index partitioned-heritability-tissue
python3 -m bioindex.main index pathway-associations
python3 -m bioindex.main index dataset-associations
python3 -m bioindex.main index genetic-correlation
