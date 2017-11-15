package org.big.bio.transformers;

import com.google.common.collect.Lists;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.big.bio.keys.BinMZKey;
import scala.Tuple2;
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
import java.util.Iterator;
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
public class IncrementalClusteringTransformer implements PairFlatMapFunction<Tuple2<BinMZKey, Iterable<ICluster>>, BinMZKey, Iterable<ICluster>> {

    ISimilarityChecker similarityChecker;

    double clusteringPrecision;

    IFunction<List<IPeak>, List<IPeak>> peakFilterFunction;

    IComparisonPredicate<ICluster> comparisonPredicate;

    public IncrementalClusteringTransformer(ISimilarityChecker similarityChecker, double clusteringPrecision, IFunction<List<IPeak>,
            List<IPeak>> peakFilterFunction, IComparisonPredicate<ICluster> comparisonPredicate){
        this.similarityChecker = similarityChecker;
        this.clusteringPrecision = clusteringPrecision;
        this.peakFilterFunction = peakFilterFunction;
        this.comparisonPredicate = comparisonPredicate;
    }

    @Override
    public Iterator<Tuple2<BinMZKey, Iterable<ICluster>>> call(Tuple2<BinMZKey, Iterable<ICluster>> binMZKeyIterableTuple2) throws Exception {

        List<Tuple2<BinMZKey, Iterable<ICluster>>> ret = new ArrayList<>();

        List<ICluster> clusterList = Lists.newArrayList(binMZKeyIterableTuple2._2());
        Collections.sort(clusterList, (o1, o2) -> Float.compare(o1.getPrecursorMz(), o2.getPrecursorMz()));

        IIncrementalClusteringEngine engine = createIncrementalClusteringEngine();

        // Add spectra to the cluster engine.
        clusterList.forEach(engine::addClusterIncremental);

        // Return the results.
        ret.add(new Tuple2<>(binMZKeyIterableTuple2._1(), engine.getClusters()));

        return ret.iterator();
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
