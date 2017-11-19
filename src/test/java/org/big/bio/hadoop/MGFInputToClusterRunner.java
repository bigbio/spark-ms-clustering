package org.big.bio.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.big.bio.clustering.pride.PRIDEClusterDefaultParameters;
import org.big.bio.keys.BinMZKey;
import org.big.bio.transformers.mappers.MGFStringToBinnedClusterMapTransformer;
import org.big.bio.transformers.mappers.MGFStringToSpectrumMapTransformer;
import org.big.bio.utils.SparkUtil;
import org.junit.Before;
import org.junit.Test;
import uk.ac.ebi.pride.spectracluster.cluster.ICluster;

import java.io.IOException;

/**
 * This code is licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * ==Overview==
 * <p>
 * This class is used to test the MGF to Cluster representation. The class allows to read the Text from the
 * file and retrieve a cluster representation rather than ISpectrum.
 *
 * <p>
 * Created by Yasset Perez-Riverol (ypriverol@gmail.com) on 31/10/2017.
 */
public class MGFInputToClusterRunner {

    private static final Logger LOGGER = Logger.getLogger(MGFInputToClusterRunner.class);
    private JavaSparkContext sparkConf;
    private String hdfsFileName;

    @Before
    public void setup(){
        sparkConf = SparkUtil.createJavaSparkContext("Test MGF Read", "local[*]");
        hdfsFileName = "./data/spectra/";
    }

    @Test
    public void readingClusterFile() throws IOException, InterruptedException {

        Class inputFormatClass = MGFInputFormat.class;
        Class keyClass = String.class;
        Class valueClass = String.class;

        Configuration hadoopConf = sparkConf.hadoopConfiguration();

        JavaPairRDD<String, String> spectraAsStrings = sparkConf.newAPIHadoopFile(hdfsFileName, inputFormatClass, keyClass, valueClass, hadoopConf);
        JavaPairRDD<BinMZKey, ICluster> spectra = spectraAsStrings.mapToPair(new MGFStringToBinnedClusterMapTransformer(sparkConf, PRIDEClusterDefaultParameters.INIT_CURRENT_BINNER_WINDOW_PROPERTY));

        boolean forceShuffle = true;
        JavaRDD<ICluster> spectraToScore = spectra.values();
        spectraToScore.coalesce(120, true);

        spectraToScore = spectraToScore.persist(StorageLevel.DISK_ONLY());

        long pairs = spectraToScore.count();
        LOGGER.info("Read  " + pairs + " records");
    }




}
