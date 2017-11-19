package org.big.bio.clustering.pride;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.big.bio.keys.BinMZKey;
import scala.Tuple2;
import uk.ac.ebi.pride.spectracluster.cluster.ICluster;

import java.util.ArrayList;
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
 * This class contains helper methods for PRIDE Cluster algorithm.
 * <p>
 * Created by ypriverol (ypriverol@gmail.com) on 07/11/2017.
 */
public class PRIDEClusterUtils {

    private static final Logger LOGGER = Logger.getLogger(PRIDEClusterUtils.class);

    /**
     * Helper function to create the List of clustering thresholds based on the initial starting threshold
     * (highest precision), the target threshold, and the number of clustering rounds to perform.
     * @param startThreshold Highest starting precision to use.
     * @param endThreshold Lowest target precision for the clustering run.
     * @param clusteringRounds Number of round of clustering to perform.
     * @return A List of thresholds used as parameter for the clustering runs.
     */
    public static List<Float> generateClusteringThresholds(Float startThreshold, Float endThreshold, int clusteringRounds) {
        List<Float> thresholds = new ArrayList<>(clusteringRounds);
        float stepSize = (startThreshold - endThreshold) / (clusteringRounds - 1);

        for (int i = 0; i < clusteringRounds; i++) {
            thresholds.add(startThreshold - (stepSize * i));
        }
        return thresholds;
    }

    /**
     * This function compute the number of clusters in a JavaPairRDD. The metrics define how many clusters
     * high quality are present in the current results .
     *
     * @param message Message to print
     * @param clusters Clusters.
     */
    public static void reportNumberOfClusters(String message, JavaPairRDD<BinMZKey, Iterable<ICluster>> clusters){
        LOGGER.info(message + clusters.count());
        JavaRDD<ICluster>  totalCluster = clusters
                .flatMapValues(cluster -> cluster)
                .map(Tuple2::_2);
        LOGGER.info(message + totalCluster.count());
    }


    /**
     * Print the final number of clusters
     *
     * @param message message to print
     * @param clusters final number of clusters
     */
    public static void reportNumberOfClusters(String message, JavaRDD<ICluster> clusters) {
        LOGGER.info(message  + clusters.count());

    }

    /**
     * Print a message and double in the LOGGER
     * @param message Message
     * @param value double value
     */
    public static void reportNumber(String message, double value) {
        LOGGER.info(message + value);
    }
}
