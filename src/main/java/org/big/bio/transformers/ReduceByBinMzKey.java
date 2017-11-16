package org.big.bio.transformers;

import com.google.common.collect.Lists;
import uk.ac.ebi.pride.spectracluster.cluster.ICluster;
import uk.ac.ebi.pride.spectracluster.engine.GreedyIncrementalClusteringEngine;
import uk.ac.ebi.pride.spectracluster.engine.IIncrementalClusteringEngine;
import uk.ac.ebi.pride.spectracluster.similarity.ISimilarityChecker;
import uk.ac.ebi.pride.spectracluster.spectrum.IPeak;
import uk.ac.ebi.pride.spectracluster.util.Defaults;
import uk.ac.ebi.pride.spectracluster.util.function.IFunction;
import uk.ac.ebi.pride.spectracluster.util.predicate.IComparisonPredicate;

import java.util.ArrayList;
import java.util.Collections;
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
 * This class
 * <p>
 * Created by ypriverol (ypriverol@gmail.com) on 16/11/2017.
 */
public class ReduceByBinMzKey implements org.apache.spark.api.java.function.Function2<Iterable<uk.ac.ebi.pride.spectracluster.cluster.ICluster>, Iterable<uk.ac.ebi.pride.spectracluster.cluster.ICluster>, Iterable<uk.ac.ebi.pride.spectracluster.cluster.ICluster>> {

    ISimilarityChecker similarityChecker;

    double clusteringPrecision;

    IFunction<List<IPeak>, List<IPeak>> peakFilterFunction;

    IComparisonPredicate<ICluster> comparisonPredicate;

    public ReduceByBinMzKey(ISimilarityChecker similarityChecker, double clusteringPrecision, IFunction<List<IPeak>,
            List<IPeak>> peakFilterFunction, IComparisonPredicate<ICluster> comparisonPredicate){
        this.similarityChecker = similarityChecker;
        this.clusteringPrecision = clusteringPrecision;
        this.peakFilterFunction = peakFilterFunction;
        this.comparisonPredicate = comparisonPredicate;
    }

    @Override
    public Iterable<ICluster> call(Iterable<ICluster> iClusters, Iterable<ICluster> iClusters2) throws Exception {

        List<ICluster> clusterList = Lists.newArrayList(iClusters2);

        clusterList.addAll(Lists.newArrayList(iClusters));

        Collections.sort(clusterList, (o1, o2) -> Float.compare(o1.getPrecursorMz(), o2.getPrecursorMz()));

        IIncrementalClusteringEngine engine = createIncrementalClusteringEngine();

        // Add spectra to the cluster engine.
        clusterList.forEach(engine::addClusterIncremental);

        return engine.getClusters();
    }

    /**
     * Return the PRIDE Cluster Incremental Engine that process all the ICluster List and return the final clusters.
     * @return IIncrementalClusteringEngine cluster engine.
     */
    private IIncrementalClusteringEngine createIncrementalClusteringEngine() {
        return new GreedyIncrementalClusteringEngine(
                similarityChecker,
                Defaults.getDefaultSpectrumComparator(),
                Defaults.getDefaultPrecursorIonTolerance(),
                clusteringPrecision,
                peakFilterFunction,
                comparisonPredicate);
    }
}
