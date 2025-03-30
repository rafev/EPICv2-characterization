# AWS processing scripts for probe characterization from EPICv1 to EPICv2
This repo contains a series of scripts with specialized functions used to characterize probe similarities of those lost when transitioning from Illumina's EPICv1 array to its new EPICv2 array. A number of heuristics were used to map propbe similaritry to lost features, and each function rank-orders suitable candidates based on those metrics.

The end goal of this analysis is to find suitable replacement probes for those lost to ensure continued operation of ML models dependant on the lost features with little to no impact on model estimates. This ensures migration to the new array retains original performance of ML models, thus preventing data drift driven failure.
