package org.big.bio.transformers.reducers;


import uk.ac.ebi.pride.spectracluster.cluster.ICluster;
import uk.ac.ebi.pride.spectracluster.engine.GreedyIncrementalClusteringEngine;
import uk.ac.ebi.pride.spectracluster.engine.IIncrementalClusteringEngine;
import uk.ac.ebi.pride.spectracluster.similarity.ISimilarityChecker;
import uk.ac.ebi.pride.spectracluster.spectrum.IPeak;
import uk.ac.ebi.pride.spectracluster.util.Defaults;
import uk.ac.ebi.pride.spectracluster.util.function.IFunction;
import uk.ac.ebi.pride.spectracluster.util.predicate.IComparisonPredicate;


import java.io.Serializable;
import java.util.List;

/**
 * This code is licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * ==Overview==
 * <p>
 * This class performs the actual clustering step on a JavaPairRDD.
 * <p>
 * Created by ypriverol (ypriverol@gmail.com) on 07/11/2017.
 */
public class IncrementalClusteringReducer implements Serializable{

    ISimilarityChecker similarityChecker;

    double clusteringPrecision;

    IFunction<List<IPeak>, List<IPeak>> peakFilterFunction;

    IComparisonPredicate<ICluster> comparisonPredicate;

    public IncrementalClusteringReducer(ISimilarityChecker similarityChecker, double clusteringPrecision, IFunction<List<IPeak>,
            List<IPeak>> peakFilterFunction, IComparisonPredicate<ICluster> comparisonPredicate){
        this.similarityChecker = similarityChecker;
        this.clusteringPrecision = clusteringPrecision;
        this.peakFilterFunction = peakFilterFunction;
        this.comparisonPredicate = comparisonPredicate;
    }

    /* Return the PRIDE Cluster Incremental Engine that process all the ICluster List and return the final clusters.
     * @return IIncrementalClusteringEngine cluster engine.
     */
    public IIncrementalClusteringEngine createIncrementalClusteringEngine() {
        return new GreedyIncrementalClusteringEngine(
                similarityChecker,
                Defaults.getDefaultSpectrumComparator(),
                Defaults.getDefaultPrecursorIonTolerance(),
                clusteringPrecision,
                peakFilterFunction,
                comparisonPredicate);
    }
}
