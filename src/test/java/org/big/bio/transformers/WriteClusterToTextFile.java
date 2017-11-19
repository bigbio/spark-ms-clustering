package org.big.bio.transformers;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.big.bio.clustering.pride.PRIDEClusterDefaultParameters;
import org.big.bio.hadoop.ClusteringFileOutputFormat;
import org.big.bio.hadoop.MGFInputFormat;
import org.big.bio.keys.BinMZKey;
import org.big.bio.keys.MZKey;
import org.big.bio.transformers.mappers.MGFStringToSpectrumMapTransformer;
import org.big.bio.transformers.mappers.PrecursorBinnerMapTransformer;
import org.big.bio.transformers.mappers.SpectrumToInitialClusterMapTransformer;
import org.big.bio.utils.SparkUtil;
import org.junit.Before;
import org.junit.Test;
import uk.ac.ebi.pride.spectracluster.cluster.ICluster;
import uk.ac.ebi.pride.spectracluster.spectrum.ISpectrum;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

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
 * Created by ypriverol (ypriverol@gmail.com) on 06/11/2017.
 */
public class WriteClusterToTextFile {

    private static final Logger LOGGER = Logger.getLogger(WriteClusterToTextFile.class);
    private String hdfsFileName;
    private String hdfsOutputFile;
    private JavaSparkContext sparkConf;

    @Before
    public void setup() throws URISyntaxException, IOException {

        URI uri = WriteClusterToTextFile.class.getClassLoader().getResource("default-spark-local.properties").toURI();
        File confFile = new File(uri);

        sparkConf = SparkUtil.createJavaSparkContext("Test Spectrum Transformation to Cluster", "local[*]" );

        Properties properties = SparkUtil.readProperties(sparkConf.hadoopConfiguration(), confFile);
        SparkUtil.addProperties(properties, sparkConf.hadoopConfiguration());

        hdfsFileName = "./data/spectra/";
        hdfsOutputFile = "./hdfs/results.clustering";
    }

    @Test
    public  void writingClusters() throws IOException, InterruptedException {

        Configuration hadoopConf = sparkConf.hadoopConfiguration();

        Class inputFormatClass = MGFInputFormat.class;
        Class keyClass = String.class;
        Class valueClass = String.class;

        JavaPairRDD<String, String> spectraAsStrings = sparkConf.newAPIHadoopFile(hdfsFileName, inputFormatClass, keyClass, valueClass, hadoopConf);

        JavaPairRDD<String, ISpectrum> spectra = spectraAsStrings.mapToPair(new MGFStringToSpectrumMapTransformer());
        LOGGER.info("Number of Spectra = " + spectra.count());

        JavaPairRDD<MZKey, ICluster> initialClusters =  spectra.mapToPair(new SpectrumToInitialClusterMapTransformer(sparkConf));

        JavaPairRDD<BinMZKey, ICluster> precursorClusters =  initialClusters.mapToPair(new PrecursorBinnerMapTransformer(sparkConf, PRIDEClusterDefaultParameters.INIT_CURRENT_BINNER_WINDOW_PROPERTY));

        JavaPairRDD<BinMZKey, Iterable<ICluster>> clusters = precursorClusters.groupByKey();

        JavaRDD<String> clusterString = clusters.flatMapToPair(new IterableClustersToCGFStringTransformer()).values();

        clusterString.saveAsTextFile(hdfsOutputFile);

        LOGGER.info("Number of Binned Clusters = " + precursorClusters.count());

    }


    @Test
    public  void writingStructuredClusters() throws IOException, InterruptedException {

        Configuration hadoopConf = sparkConf.hadoopConfiguration();

        Class inputFormatClass = MGFInputFormat.class;
        Class keyClass = String.class;
        Class valueClass = String.class;

        JavaPairRDD<String, String> spectraAsStrings = sparkConf.newAPIHadoopFile(hdfsFileName, inputFormatClass, keyClass, valueClass, hadoopConf);

        JavaPairRDD<String, ISpectrum> spectra = spectraAsStrings.mapToPair(new MGFStringToSpectrumMapTransformer());
        LOGGER.info("Number of Spectra = " + spectra.count());

        JavaPairRDD<MZKey, ICluster> initialClusters =  spectra.mapToPair(new SpectrumToInitialClusterMapTransformer(sparkConf));
        JavaPairRDD<BinMZKey, ICluster> precursorClusters =  initialClusters.mapToPair(new PrecursorBinnerMapTransformer(sparkConf, PRIDEClusterDefaultParameters.INIT_CURRENT_BINNER_WINDOW_PROPERTY));

        JavaPairRDD<BinMZKey, Iterable<ICluster>> clusters = precursorClusters.groupByKey();
        clusters.flatMapToPair(new IterableClustersToCGFStringTransformer()).saveAsNewAPIHadoopFile(hdfsOutputFile, String.class, String.class, ClusteringFileOutputFormat.class);


        LOGGER.info("Number of Binned Clusters = " + precursorClusters.count());

    }




}
