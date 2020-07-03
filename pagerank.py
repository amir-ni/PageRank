from __future__ import print_function

import re
import sys
from operator import add
from ast import literal_eval

from pyspark import StorageLevel
from pyspark.sql import SparkSession


def edgeParser(line):
    """Parses an input file line into edge format."""

    parts = re.split(r'\s+', line)

    try:
        source = str(parts[0])
        destination = str(parts[1])
        if len(parts) == 3 and parts[2] != '':
            weight = float(parts[2])
        else:
            weight = 1.0
    except Exception as parsingException:
        raise ValueError('input file was not in correct format') from parsingException
    
    return (source, (destination, weight))


def weightsNormalizer(destinations):
    """Normalizes each vertex's out-going weight sum to one."""

    destinations = list(destinations)
    newDestinations = []
    sum = 0

    for destination, weight in destinations:
        sum += weight

    for destination, weight in destinations:
        newDestinations.append((destination, weight / sum))

    return newDestinations


def matrixExtender(rowVector, teleportProbability, n, augmentedColumnKey):
    """Builds augmented transition matrix."""

    needSurplus = True
    tempRowVector = []
    finalRowVector = []

    for destination, weight in rowVector:
        weight *= (1 - teleportProbability)
        tempRowVector.append((destination, weight))

    for destination, weight in tempRowVector:
        if destination == augmentedColumnKey:
            needSurplus = False
            weight += (teleportProbability / n)
        finalRowVector.append((destination, weight))

    if needSurplus:
        finalRowVector.append((augmentedColumnKey, teleportProbability / n))

    return finalRowVector


def outerProduct(row):
    """Calculates outer product of vector and matrix."""

    source, (multiplier, destinations) = row
    finalRow = []
    hasOwnKey = False

    for destination, weight in destinations:
        weight *= multiplier
        finalRow.append((destination, weight))
        if destination == source:
            hasOwnKey = True

    if not hasOwnKey:
        finalRow.append((source, 0))

    return finalRow


if __name__ == "__main__":
    if len(sys.argv) != 5 and len(sys.argv) != 6:
        print("Usage: pagerank <file> <teleport-probability> <max-iterations> <convergence-threshold> <initial-priors:optional>", file=sys.stderr)
        sys.exit(-1)

    resultPath = './result'
    numPartitions = 4
    defaultPersistenceLevel = StorageLevel.MEMORY_AND_DISK_2 
    augmentedColumnKey = -1

    # Initialize the spark context.
    spark = SparkSession\
        .builder\
        .appName("PageRank")\
        .getOrCreate()

    sc = spark.sparkContext
    sc.setCheckpointDir('./checkpoints')

    initialPriorsPath = None
    try:
        filePath = str(sys.argv[1])
        teleportProb = sc.broadcast(float(sys.argv[2]))
        maxIterations = int(sys.argv[3])
        convergenceThreshold = float(sys.argv[4])
        initialPriorsPath = str(sys.argv[5]) if len(sys.argv) == 6 else None
    except Exception as argumentException:
        raise ValueError('runtime arguments were not in correct format') from argumentException

    # Loads in input file and partition it by source. It should be in format of:
    #   source   destination  weight(optional)
    #   source   destination  weight(optional)
    #   source   destination  weight(optional)
    #   ...
    weightedEdges = sc.textFile(filePath)\
        .map(edgeParser)\
        .filter(lambda x: x[0] != x[1][0])\
        .partitionBy(numPartitions)\
        .persist(defaultPersistenceLevel)

    # Find all non dangling vertices
    nonDangles = weightedEdges.map(lambda x: x[0])\
        .distinct()\
        .persist(defaultPersistenceLevel)

    # Find all vertices ids
    vertices = weightedEdges.flatMap(lambda r: [r[0], r[1][0]])\
        .distinct()\
        .coalesce(numPartitions)\
        .persist(defaultPersistenceLevel)

    # Subtract non dangling vertices from all vertices to find dangling vertices
    dangles = vertices.subtract(nonDangles)\
        .persist(defaultPersistenceLevel)

    # Unpersist non dangling vertices RDD because we need it no more
    nonDangles.unpersist()

    # Normalize edges weights
    edges = weightedEdges.groupByKey()\
        .mapValues(weightsNormalizer)\
        .persist(defaultPersistenceLevel)

    # Unpersist non normalized edges RDD because we need it no more
    weightedEdges.unpersist()

    # Count total number of vertices 
    numVertices = sc.broadcast(vertices.count())

    # Create initial pagerank vector
    if initialPriorsPath is None:
        newVertices = vertices.map(lambda x: (x, 1 / numVertices.value))\
            .coalesce(numPartitions)\
            .persist(defaultPersistenceLevel)
    else:
        newVertices = sc.textFile(initialPriorsPath+"/part-*")\
            .map(literal_eval)\
            .coalesce(numPartitions)\
            .persist(defaultPersistenceLevel)

    # Unpersist vertice's ids RDD because we need it no more
    vertices.unpersist()

    # Create sparse random-walker markov matrix from edges but with one extra column for dangling nodes
    extendedMarkovMatrix = edges.union(
        dangles.map(lambda x: (x, [(augmentedColumnKey, 1 / (numVertices.value - 1)), (x, -1 / (numVertices.value - 1))])))\
            .mapValues(lambda x: matrixExtender(x, teleportProb.value, numVertices.value, augmentedColumnKey))\
            .coalesce(numPartitions)\
            .persist(defaultPersistenceLevel)

    #  Unpersist RDDs that are not used in iterations
    edges.unpersist()
    dangles.unpersist()

    # Checkpoint matrix to prevent dataloss in case of failure 
    extendedMarkovMatrix.checkpoint()

    hasConverged = False
    numIterations = 0

    while (not hasConverged) and (numIterations < maxIterations):
        previousVertices = newVertices
        # Calculate outre product of markov matrix and pagerank vector
        newVertices = previousVertices.join(extendedMarkovMatrix)\
            .flatMap(outerProduct)\
            .coalesce(numPartitions)\
            .reduceByKey(add)\
            .persist(defaultPersistenceLevel)
        # Calculate missing weight from dangling nodes
        missingSumPerVertex = sc.broadcast(newVertices.filter(lambda x: x[0] == augmentedColumnKey).map(lambda x: x[1]).reduce(add))
        # Add missing weight to vertices
        newVertices = newVertices.mapValues(lambda x: x + missingSumPerVertex.value)\
            .filter(lambda x: x[0] != augmentedColumnKey)\
            .coalesce(numPartitions)\
            .persist(defaultPersistenceLevel)
        # Calculate norm1 difference of new pagerank vector and previous one
        delta = newVertices.join(previousVertices)\
            .map(lambda x: abs(x[1][0] - x[1][1]))\
            .reduce(add)
        # Unpersist pervious iteration's pagerank vector
        previousVertices.unpersist()
        newVertices.checkpoint()
        # Check if convergence threshold is reached
        hasConverged = (delta < convergenceThreshold)
        numIterations += 1

    newVertices.saveAsTextFile(resultPath)
    spark.stop()