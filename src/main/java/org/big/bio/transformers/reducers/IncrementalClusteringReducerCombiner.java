package org.big.bio.transformers.reducers;

import com.google.common.collect.Lists;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;
import uk.ac.ebi.pride.spectracluster.cluster.ICluster;
import uk.ac.ebi.pride.spectracluster.engine.IIncrementalClusteringEngine;
import uk.ac.ebi.pride.spectracluster.similarity.ISimilarityChecker;
import uk.ac.ebi.pride.spectracluster.spectrum.IPeak;
import uk.ac.ebi.pride.spectracluster.util.function.IFunction;
import uk.ac.ebi.pride.spectracluster.util.predicate.IComparisonPredicate;

import java.util.ArrayList;
import java.util.Collection;
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
 * Created by ypriverol (ypriverol@gmail.com) on 19/11/2017.
 */
public class IncrementalClusteringReducerCombiner extends IncrementalClusteringReducer
        implements Function2<Iterable<ICluster>, ICluster, Iterable<ICluster>> {

    /**
     * Incremental Clustering that merge an ICluster with a list of cluster.
     * @param similarityChecker Similarity Checker
     * @param clusteringPrecision clustering precision
     * @param peakFilterFunction peak filtering function
     * @param comparisonPredicate comparison predicate
     */
    public IncrementalClusteringReducerCombiner(ISimilarityChecker similarityChecker, double clusteringPrecision, IFunction<List<IPeak>, List<IPeak>> peakFilterFunction, IComparisonPredicate<ICluster> comparisonPredicate) {
        super(similarityChecker, clusteringPrecision, peakFilterFunction, comparisonPredicate);
    }

    /**
     * Ad a cluster to the List of clusters and merge them.
     * @param iClusters list of clusters
     * @param iCluster cluster
     * @return list of clusters
     * @throws Exception Error if the cluster can't be merge
     */

    @Override
    public Iterable<ICluster> call(Iterable<ICluster> iClusters, ICluster iCluster) throws Exception {

        //Init the clusters

        List<ICluster> clusterList = new ArrayList<>((Collection<? extends ICluster>)iClusters);
        clusterList.add(iCluster);
        Collections.sort(clusterList, (o1, o2) -> Float.compare(o1.getPrecursorMz(), o2.getPrecursorMz()));

        // Add spectra to the cluster engine.
        IIncrementalClusteringEngine engine = createIncrementalClusteringEngine();
        clusterList.forEach(engine::addClusterIncremental);

        // Return the results.
        return engine.getClusters();
    }
}
