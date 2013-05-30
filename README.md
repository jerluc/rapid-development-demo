## Rapid, Interactive Development of Hadoop Applications

This repository houses examples from my session on rapidly developing software on the Hadoop technology stack through the use of commonly-found test utilities.

## Demos/Examples
Included in this repository are sources for three demo/example M/R jobs:

### Filter Records Map-only job
This job demonstrates a simple map-only job that filters out any record which does not contain an "ACTIVE" status. The job is developed by using MRUnit for testing.

### Join/CoGroup Map/Reduce job
This job uses the identity mapper with the JoinReducer to perform a reduce-side full join/cogroup operation on any given key. The job is developed by using MRUnit for testing.

### MultipleOutputs Map/Reduce job
This job demonstrates the MultipleOutputs functionality built into the Hadoop MapReduce framework. In the map-phase, this job partitions the data by the first field in the value (a category ID in this scenario). Then, in the reduce phase, we flush all of these records out to their appropriate output partition directories based on the key (also category ID). This job must use MiniMrCluster to achieve such functionality, as MRUnit cannot support multiple outputs.
