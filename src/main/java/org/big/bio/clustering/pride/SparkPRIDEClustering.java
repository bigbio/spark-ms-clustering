package org.big.bio.clustering.pride;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.big.bio.clustering.IMSClustering;
import org.big.bio.clustering.MSClustering;
import org.big.bio.hadoop.ClusteringFileOutputFormat;
import org.big.bio.hadoop.MGFInputFormat;
import org.big.bio.keys.BinMZKey;
import org.big.bio.qcontrol.QualityControlUtilities;
import org.big.bio.transformers.*;
import org.big.bio.transformers.mappers.IterableClustersToBinnerFlatMapTransformer;
import org.big.bio.transformers.mappers.MGFStringToBinnedClusterMapTransformer;
import org.big.bio.transformers.reducers.IncrementalClusteringReducerCombiner;
import org.big.bio.transformers.reducers.IncrementalClusteringReducerMerger;
import org.big.bio.utils.SparkUtil;
import scala.Tuple2;
import uk.ac.ebi.pride.spectracluster.cluster.ICluster;
import uk.ac.ebi.pride.spectracluster.similarity.ISimilarityChecker;
import uk.ac.ebi.pride.spectracluster.util.predicate.IComparisonPredicate;
import uk.ac.ebi.pride.spectracluster.util.predicate.cluster_comparison.ClusterShareMajorPeakPredicate;
import uk.ac.ebi.pride.spectracluster.util.predicate.cluster_comparison.IsKnownComparisonsPredicate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


/**
 * The SparkML Clustering KMeans used a Bisecting Kmeans approach as hierarchical clustering approached.
 * The input of the algorithm is the folder that contains all the Projects and corresponding spectra.
 *
 * @author Yasset Perez-Riverol
 *
 */
public class SparkPRIDEClustering extends MSClustering {

    // Logger
    private static final Logger LOGGER = Logger.getLogger(SparkPRIDEClustering.class);

    // Default parameters for the algorithm
    private static PRIDEClusterDefaultParameters defaultParameters = new PRIDEClusterDefaultParameters();

    //Application Spark PRIDE Clustering
    private static String APPLICATION_NAME = "SparkPRIDEClustering";

    /**
     * SparkClustering algorithm created from conf file and default properties files.
     * @param confFile Configuration file
     * @param properties default properties.
     * @throws IOException Error creating the Context or reading the properties.
     */
    public SparkPRIDEClustering(String confFile, Properties properties) throws IOException {
        super(APPLICATION_NAME, confFile, properties);
    }

    public SparkPRIDEClustering() {
        super(APPLICATION_NAME);
    }

    /**
     * Main method for running cluster.
     *
     * parameters:
     *  --conf: Configuration File with all paramters for the algorithm.
     *  --input-path: Input path
     *  --output-path: Output Path
     *
     * @param args arguments parameters for PRIDE Cluster algorithm
     *
     */
    public static void main(String[] args) {

        // Clustering method.
        IMSClustering clusteringMethod;

        try {
            // Parse the corresponding parameters of the algorithm
            CommandLine cmd = MSClustering.parseCommandLine(args, MSClustering.getCLIParameters());

            if(!cmd.hasOption("i") || !cmd.hasOption("o")){
                MSClustering.printHelpCommands();
                System.exit( -1 );
            }

            if(!cmd.hasOption("c")){
                LOGGER.info("The algorithm will run in local mode");
                clusteringMethod = new SparkPRIDEClustering();
            }else{
                clusteringMethod = new SparkPRIDEClustering(cmd.getOptionValue("c"), defaultParameters.getProperties());
            }

            // Input Path
            String inputPath = cmd.getOptionValue("i");

            //Output Path
            String hdfsOutputFile = cmd.getOptionValue("o");

            Class inputFormatClass = MGFInputFormat.class;
            Class keyClass = String.class;
            Class valueClass = String.class;

            JavaPairRDD<String, String> spectraAsStrings = clusteringMethod.context().newAPIHadoopFile(inputPath, inputFormatClass, keyClass, valueClass, clusteringMethod.context().hadoopConfiguration());
            SparkUtil.collectLogCount("Number of Spectra To Cluster = ", spectraAsStrings);

            // Process the Spectra Files and convert them into BinMZKey Hash map. Each entry correspond to a "unique" precursor mass.
            JavaPairRDD<BinMZKey, ICluster> spectra = spectraAsStrings
                    .mapToPair(new MGFStringToBinnedClusterMapTransformer(clusteringMethod.context(), PRIDEClusterDefaultParameters.INIT_CURRENT_BINNER_WINDOW_PROPERTY));
            SparkUtil.collectLogCount("Number of Binned Precursors = " , spectra);

            //Thresholds for the refinements of the results
            List<Float> thresholds = PRIDEClusterUtils.generateClusteringThresholds(Float.parseFloat(clusteringMethod.getProperty(PRIDEClusterDefaultParameters.CLUSTER_START_THRESHOLD_PROPERTY)),
                    Float.parseFloat(clusteringMethod.getProperty(PRIDEClusterDefaultParameters.CLUSTER_END_THRESHOLD_PROPERTY)), Integer.parseInt(clusteringMethod.getProperty(PRIDEClusterDefaultParameters.CLUSTERING_ROUNDS_PROPERTY)));


            // The first step is to create the Major comparison predicate.
            IComparisonPredicate<ICluster> comparisonPredicate = new ClusterShareMajorPeakPredicate(Integer.parseInt(clusteringMethod.getProperty(PRIDEClusterDefaultParameters.MAJOR_PEAK_COUNT_PROPERTY)));

            // Create the similarity Checker.
            ISimilarityChecker similarityChecker = PRIDEClusterDefaultParameters.getSimilarityCheckerFromConfiguration(clusteringMethod.context().hadoopConfiguration());
            double originalPrecision = Float.parseFloat(clusteringMethod.getProperty(PRIDEClusterDefaultParameters.CLUSTER_START_THRESHOLD_PROPERTY));

            // Group the ICluster by BinMzKey.
            JavaPairRDD<BinMZKey, Iterable<ICluster>> binnedPrecursors = spectra
                    .combineByKey(cluster -> {
                                List<ICluster> clusters = new ArrayList<>();
                                clusters.add(cluster);
                                return clusters;
                            }, new IncrementalClusteringReducerCombiner(similarityChecker, originalPrecision, null, comparisonPredicate)
                            , new IncrementalClusteringReducerMerger(similarityChecker, originalPrecision, null, comparisonPredicate));

            //Number of Clusters after the first iteration
            PRIDEClusterUtils.reportNumberOfClusters("Number of Clusters after ClusterShareMajorPeakPredicate = ", binnedPrecursors);

            // The first step is to create the Major comparison predicate.
            for(Float threshold: thresholds){

                spectra = binnedPrecursors.flatMapToPair(new IterableClustersToBinnerFlatMapTransformer(clusteringMethod.context(), PRIDEClusterDefaultParameters.BINNER_WINDOW_PROPERTY));

                //Predicate.
                comparisonPredicate = new IsKnownComparisonsPredicate();

                // Group the ICluster by BinMzKey.
                binnedPrecursors = spectra
                        .combineByKey(cluster -> {
                                    List<ICluster> clusters = new ArrayList<>();
                                    clusters.add(cluster);
                                    return clusters;
                                }, new IncrementalClusteringReducerCombiner(similarityChecker, originalPrecision, null, comparisonPredicate)
                                , new IncrementalClusteringReducerMerger(similarityChecker, originalPrecision, null, comparisonPredicate));

                // Cluster report for iteration
                PRIDEClusterUtils.reportNumberOfClusters("Number of Clusters after IncrementalClusteringReducer , Thershold " + threshold + " = ", binnedPrecursors);
            }

            // Final Number of Clusters
            JavaRDD<ICluster> finalClusters = binnedPrecursors
                    .flatMapValues(cluster -> cluster)
                    .map(Tuple2::_2);
            PRIDEClusterUtils.reportNumberOfClusters("Final Number of Clusters  = ", finalClusters);

            // Print the global Quality
            PRIDEClusterUtils.reportNumber("Global cluster quality = ", QualityControlUtilities.clusteringGlobalQuality(finalClusters, 3));

            // Print the global Accuracy
            PRIDEClusterUtils.reportNumber("Global cluster Accuracy = ", QualityControlUtilities.clusteringGlobalAccuracy(finalClusters, 3));


            // The export can be done in two different formats CGF or Clustering (JSON)
            JavaPairRDD<String, String> finalStringClusters;
            if(Boolean.parseBoolean(clusteringMethod.getProperty(PRIDEClusterDefaultParameters.CLUSTER_EXPORT_FORMAT_CDF_PROPERTY)))
                finalStringClusters = binnedPrecursors.flatMapToPair(new IterableClustersToCGFStringTransformer());
            else
                finalStringClusters = binnedPrecursors.flatMapToPair(new IterableClustersToJSONStringTransformer());

            // Final export of the results
            finalStringClusters.saveAsNewAPIHadoopFile(hdfsOutputFile, String.class, String.class, ClusteringFileOutputFormat.class);

        } catch (ParseException | IOException e) {
            MSClustering.printHelpCommands();
            e.printStackTrace();
        }
    }



}