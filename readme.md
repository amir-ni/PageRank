# PageRank in PySpark

*This implementation is based on Spark's standard RDD API.*

## Features

- Fast iterations
- Parameterised teleport probability
- Supports arbitrary priors
- Supports arbitrary vertices keys
- Supports and removes self-loops
- Supports weighted and non-weighted edges
- Supports not normalized weighted edges
- Supports dangling vertices, i.e., vertices with no outgoing edge
- Supports stopping condition on both iteration limit and convergence threshold
- Supports multiple arcs, i.e., edges with the same source and destination vertices
- Edges data is partitioned by source vertices in order to reduce required shuffle
- Implemented RDD checkpointing that allows a driver to be restarted on failure with previously computed state

## How to use

```console
pagerank <file> <teleport-probability> <max-iterations> <convergence-threshold> <initial-priors:optional>
```

### example

```console
bin/spark-submit pagerank.py ./sample_input.txt 0.15 3 0.01
```

Results RDD is final pagerank vector and will be saved as a text file using string representations of elements in **results** folder.

- Results saving path can be changed in line 93
- Workers count can be changed in line 94
- Default persistence level can be changed in line 95
- There is an augmented column in the transition matrix to accelerate the computations, and its key shouldn't exist in graph vertices names. default key is **-1** but you can change it in line 96

### input file format

```plaintext
source   destination  weight(optional)
source   destination  weight(optional)
source   destination  weight(optional)
```

- weights are optional and should be non-negative decimals
- each node should have at least one out-going or in-going edge
  - in case the input graph has isolated vertices, insert self-loops for them in the input file in order to include them in computations

There is also an optional argument *initial-prior* that could point to a saved RDD path in order to initialize vertex priors. its format should be same as saved result RDD:

```plaintext
('1', 0.21323581349206347)
('2', 0.2938243905895691)
('3', 0.21263633219954647)
('4', 0.28030346371882076)
```

This argument can be used in order to recover checkpointed calculations in case of failure

## Authors

- [Amirhossein Nadiri](https://github.com/amir-ni)

## License

This project is open-source software licensed under the [MIT license](https://opensource.org/licenses/MIT)