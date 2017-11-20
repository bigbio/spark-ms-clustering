package org.big.bio.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Properties;

/**
 * This is a utility class to create JavaSparkContext 
 * and other objects required by Spark. There are many 
 * ways to create JavaSparkContext object. Here we offer 
 * 2 ways to create it:
 *
 *   1. by using YARN's resource manager host name
 *
 *   2. by using spark master URL, which is expressed as: 
 *
 *           spark://<spark-master-host-name>:7077
 *
 * @author Yasset Perez-Riverol
 *
 */
public class SparkUtil {

    private static final Logger LOGGER = Logger.getLogger(SparkUtil.class);


    /**
     * The application name is provided and the node where this will run, it works
     * for local machines, foe example:
     * local[*]
     *
     * @param applicationName Application Name
     * @param master Machine where this application will run
     * @return JavaSparkContext
     */
    public static JavaSparkContext createJavaSparkContext(String applicationName, String master){
      SparkConf conf = new SparkConf().setAppName(applicationName).setMaster(master);
      // This is need to re-write the corresponding partition.
      conf.set("spark.hadoop.validateOutputSpecs", "false");
      return new JavaSparkContext(conf);
   }

    /**
     * The application name. The node or machine where the algorithm will run needs to be
     * provided by the config file.
     *
     * @param applicationName Application Name
     * @return JavaSparkContext
     */
   public static JavaSparkContext createJavaSparkContextWithFile(String applicationName, String confFile){
       SparkConf conf = new SparkConf().setAppName(applicationName);
        // This is need to re-write the corresponding partition.
       JavaSparkContext sparkContext = new JavaSparkContext("local[*]", applicationName);
       sparkContext.addFile(confFile);

       return sparkContext;
    }


    /**
     * The constructor create a context based in the application name, a Configuration file and the algorithm default properties.
     * The confFile has priority over the properties, them each property from properties is added to the final properties.
     * @param applicationName The application name
     * @param confFile Configuration file with all parameters
     * @param properties default properties.
     * @return JavaSparkContext the Spark Context.
     * @throws IOException Error Reading the properties file.
     */
    public static JavaSparkContext createJavaSparkContextWithFile(String applicationName, String confFile, Properties properties) throws IOException {
        SparkConf conf = new SparkConf().setAppName(applicationName);
        // This is need to re-write the corresponding partition.
        JavaSparkContext sparkContext = new JavaSparkContext("local[*]", applicationName);

        Properties confProperties = SparkUtil.readProperties(sparkContext.hadoopConfiguration(), new File(confFile));
        properties.forEach( (key, value) ->{
            if(confProperties.get(key) == null){
                confProperties.setProperty(key.toString(), value.toString());
            }
        });
        SparkUtil.addProperties(confProperties, sparkContext.hadoopConfiguration());
        sparkContext.addFile(confFile);
        return sparkContext;
    }

    /**
     * This function allow to persist a JavaPAirRDD and print their count.
     *
     * @param message Message to Print in the LOGGER information.
     * @param inp     RDD Object
     * @return Same RDD persistent.
     */
    @Nonnull
    public static <K extends Serializable, V extends Serializable> JavaPairRDD<K, V> persistAndCount(@Nonnull final String message, @Nonnull final JavaPairRDD<K, V> inp) {
        JavaPairRDD<K, V> ret = persist(inp);
        long count = ret.count();
        LOGGER.debug(message + " = " + count);
        return ret;
    }

    /**
     * This made the RDD persitant in DISK ONLY.
     *
     * @param inp RDD
     * @return Same RDD but DISK Persistant
     */
    @Nonnull
    public static <K, V> JavaPairRDD<K, V> persist(@Nonnull final JavaPairRDD<K, V> inp) {
        return  persist(inp, StorageLevel.DISK_ONLY());
    }
    /**
     * persist in the best way - saves remembering which storage level
     *
     * @param inp JavaPairRDD
     * @return return a persistant PairRDD
     */
    @Nonnull
    public static <K, V> JavaPairRDD<K, V> persist(@Nonnull final JavaPairRDD<K, V> inp,StorageLevel storageLevel) {
        return inp.persist(storageLevel);
    }

    /**
     * Load property files and configuration files and add them to conf.
     * @param conf Hadoop Configuration.
     * @param files Configuration Files
     * @return Properties
     * @throws IOException IOerror Reading the properties file.
     */
    public static Properties readProperties(Configuration conf, File... files) throws IOException {

        org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(conf);
        InputStream inputStream;

        Properties properties = new Properties();
        for(File file: files){
            inputStream = fs.open(new Path(file.toString()));
            properties.load(inputStream);
        }
        return properties;
    }

    /**
     * Add Properties to the Hadoop Configuration File.
     * @param properties properties
     * @param conf Configuration
     */
    public static void addProperties(Properties properties, Configuration conf){
        if(properties != null && conf != null){
            properties.forEach( (key, value) -> conf.set(key.toString(), value.toString()));
        }
    }

    /**
     * This method print for an RDD the count of entries.
     * @param message Message that will be added as a prefix of the Logger Message
     * @param rdd RDD with the corresponding count.
     */
    public static void collectLogCount(String message, JavaRDDLike rdd){
        LOGGER.info(message + rdd.count());
    }

    /**
     * Log a Number and message in the system.
     * @param message message to Log
     * @param number Number to Log
     */
    public static void logNumber(String message, int number) {
        LOGGER.info(message + number);
    }
}
