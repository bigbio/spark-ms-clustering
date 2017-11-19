package org.big.bio.transformers.reducers;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function2;
import org.big.bio.transformers.mappers.MGFStringToBinnedClusterMapTransformer;
import scala.tools.nsc.backend.opt.Inliners;
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
public class IncrementalClusteringReducerMerger extends IncrementalClusteringReducer
        implements Function2<Iterable<ICluster>, Iterable<ICluster>, Iterable<ICluster>> {

    private static final Logger LOGGER = Logger.getLogger(IncrementalClusteringReducerMerger.class);

    /**
     * The Incremental clustering Constructor
     * @param similarityChecker Similarity Checker
     * @param clusteringPrecision Clustering precision double
     * @param peakFilterFunction filters for the Peak Lists
     * @param comparisonPredicate Type of function to compare the spectra
     */
    public IncrementalClusteringReducerMerger(ISimilarityChecker similarityChecker, double clusteringPrecision, IFunction<List<IPeak>, List<IPeak>> peakFilterFunction, IComparisonPredicate<ICluster> comparisonPredicate) {
        super(similarityChecker, clusteringPrecision, peakFilterFunction, comparisonPredicate);
    }


    /**
     * Merge two list of clusters into one
     * @param iClusters1 First list of Clusters
     * @param iClusters2 Second list of clusters
     * @return result lis of clusters
     * @throws Exception
     */
    @Override
    public Iterable<ICluster> call(Iterable<ICluster> iClusters1, Iterable<ICluster> iClusters2) throws Exception {

        List<ICluster> clusterList = new ArrayList<>((Collection<? extends ICluster>)iClusters1);
        clusterList.addAll((Collection<? extends ICluster>) iClusters2);

        Collections.sort(clusterList, (o1, o2) -> Float.compare(o1.getPrecursorMz(), o2.getPrecursorMz()));

        // Add spectra to the cluster engine.
        IIncrementalClusteringEngine engine = createIncrementalClusteringEngine();
        clusterList.forEach(engine::addClusterIncremental);

        Collection<ICluster> results = engine.getClusters();
        LOGGER.info("Original Clusters -- " + clusterList.size() + "Final Clusters -- " + results.size());

        // Return the results.
        return results;
    }
}


