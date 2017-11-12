package org.big.bio.clustering.pride;

import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.big.bio.clustering.IMSClustering;
import org.big.bio.hadoop.ClusteringFileOutputFormat;
import org.big.bio.hadoop.MGFileFInputFormat;
import org.big.bio.keys.BinMZKey;
import org.big.bio.qcontrol.QualityControlUtilities;
import org.big.bio.transformers.*;
import org.big.bio.utils.SparkUtil;
import org.junit.Before;
import org.junit.Test;
import uk.ac.ebi.pride.spectracluster.cluster.ICluster;
import uk.ac.ebi.pride.spectracluster.similarity.ISimilarityChecker;
import uk.ac.ebi.pride.spectracluster.util.predicate.IComparisonPredicate;
import uk.ac.ebi.pride.spectracluster.util.predicate.cluster_comparison.ClusterShareMajorPeakPredicate;
import uk.ac.ebi.pride.spectracluster.util.predicate.cluster_comparison.IsKnownComparisonsPredicate;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
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
 * This class test the main PRIDE Cluster algorithm.
 * <p>
 * Created by ypriverol (ypriverol@gmail.com) on 09/11/2017.
 */
public class SparkPRIDEClusteringTest {

    private String hdfsFileName;
    private String hdfsOutputFile;
    IMSClustering clusteringMethod;

    // Default parameters for the algorithm
    private static PRIDEClusterDefaultParameters defaultParameters = new PRIDEClusterDefaultParameters();

    @Before
    public void setup() throws URISyntaxException, IOException {

        URI uri = WriteClusterToTextFile.class.getClassLoader().getResource("default-spark-local.properties").toURI();
        File confFile = new File(uri);

        clusteringMethod = new SparkPRIDEClustering(confFile.getAbsolutePath(), defaultParameters.getProperties());
        hdfsFileName = "./data/spectra/";
        hdfsOutputFile = "./hdfs/clustering";
    }

    @Test
    public void prideClusteringAlgorithm() throws Exception {

        Class inputFormatClass = MGFileFInputFormat.class;
        Class keyClass = String.class;
        Class valueClass = String.class;

            // Read the corresponding Spectra from the File System.
        JavaPairRDD<Text, Text> spectraAsStrings = clusteringMethod.context().newAPIHadoopFile(hdfsFileName, inputFormatClass, keyClass, valueClass, clusteringMethod.context().hadoopConfiguration());
        SparkUtil.collectLogCount("Number of Spectra", spectraAsStrings);

        // Process the Spectra Files and convert them into BinMZKey Hash map. Each entry correspond to a "unique" precursor mass.
        JavaPairRDD<BinMZKey, ICluster> spectra = spectraAsStrings
                .flatMapToPair(new MGFStringToSpectrumTransformer())
                .flatMapToPair(new SpectrumToInitialClusterTransformer(clusteringMethod.context()))
                .flatMapToPair(new PrecursorBinnerTransformer(clusteringMethod.context()));
        SparkUtil.collectLogCount("Number of Binned Precursors" , spectra);

        // Group the ICluster by BinMzKey.
        JavaPairRDD<BinMZKey, Iterable<ICluster>> binnedPrecursors = spectra.groupByKey();
        SparkUtil.collectLogCount("Number of Unique Binned Precursors", binnedPrecursors);

        // The first step is to create the Major comparison predicate.
        IComparisonPredicate<ICluster> comparisonPredicate = new ClusterShareMajorPeakPredicate(Integer.parseInt(clusteringMethod.getProperty(PRIDEClusterDefaultParameters.MAJOR_PEAK_COUNT_PROPERTY)));

        // Create the similarity Checker.
        ISimilarityChecker similarityChecker = PRIDEClusterDefaultParameters.getSimilarityCheckerFromConfiguration(clusteringMethod.context().hadoopConfiguration());
        double originalPrecision = Float.parseFloat(clusteringMethod.getProperty(PRIDEClusterDefaultParameters.CLUSTER_START_THRESHOLD_PROPERTY));

        binnedPrecursors = binnedPrecursors.flatMapToPair(new IncrementalClusteringTransformer(similarityChecker, originalPrecision, null, comparisonPredicate));

        //Number of Clusters after the first iteration
        PRIDEClusterUtils.reportNumberOfClusters(binnedPrecursors);


        //Thresholds for the refinements of the results
        List<Float> thresholds = PRIDEClusterUtils.generateClusteringThresholds(Float.parseFloat(clusteringMethod.getProperty(PRIDEClusterDefaultParameters.CLUSTER_START_THRESHOLD_PROPERTY)),
                Float.parseFloat(clusteringMethod.getProperty(PRIDEClusterDefaultParameters.CLUSTER_END_THRESHOLD_PROPERTY)), Integer.parseInt(clusteringMethod.getProperty(PRIDEClusterDefaultParameters.CLUSTERING_ROUNDS_PROPERTY)));

        // The first step is to create the Major comparison predicate.
        for(Float threshold: thresholds){

            comparisonPredicate = new IsKnownComparisonsPredicate();

            // Create the similarity Checker.
            similarityChecker = PRIDEClusterDefaultParameters.getSimilarityCheckerFromConfiguration(clusteringMethod.context().hadoopConfiguration());
            binnedPrecursors = binnedPrecursors.flatMapToPair(new IncrementalClusteringTransformer(similarityChecker, threshold, null, comparisonPredicate));

            // Cluster report for iteration
            PRIDEClusterUtils.reportNumberOfClusters(binnedPrecursors);
        }

        JavaRDD<ICluster> filteredClusters = binnedPrecursors
                .flatMapValues(cluster -> cluster)
                .map(cluster -> cluster._2())
                .filter(cluster -> QualityControlUtilities.avgIdentifiedRatio(cluster) > 0.70);

        PRIDEClusterUtils.reportNumberOfClusters(filteredClusters);

        // The export can be done in two different formats CGF or Clustering (JSON)
        JavaPairRDD<String, String> finalStringClusters;
        if(Boolean.parseBoolean(clusteringMethod.getProperty(PRIDEClusterDefaultParameters.CLUSTER_EXPORT_FORMAT_CDF_PROPERTY)))
            finalStringClusters = binnedPrecursors.flatMapToPair(new IterableClustersToCGFStringTransformer());
        else
            finalStringClusters = binnedPrecursors.flatMapToPair(new IterableClustersToJSONStringTransformer());

        // Final export of the results
        finalStringClusters.saveAsNewAPIHadoopFile(hdfsOutputFile, String.class, String.class, ClusteringFileOutputFormat.class);

    }

}