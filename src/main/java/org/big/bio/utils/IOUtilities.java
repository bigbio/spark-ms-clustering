package org.big.bio.utils;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.big.bio.hadoop.MGFInputFormat;
import uk.ac.ebi.pride.spectracluster.cluster.ICluster;
import uk.ac.ebi.pride.spectracluster.io.CGFClusterAppender;
import uk.ac.ebi.pride.spectracluster.io.DotClusterClusterAppender;
import uk.ac.ebi.pride.spectracluster.io.MGFSpectrumAppender;
import uk.ac.ebi.pride.spectracluster.io.ParserUtilities;
import uk.ac.ebi.pride.spectracluster.spectrum.*;


import java.io.LineNumberReader;
import java.io.StringReader;


public final class IOUtilities {

    /**
     * Convert spectrum to string
     *
     * @param spectrum given spectrum
     * @return string represents spectrum
     */
    public static String convertSpectrumToMGFString(ISpectrum spectrum) {
        StringBuilder sb = new StringBuilder();
        MGFSpectrumAppender.INSTANCE.appendSpectrum(sb, spectrum);
        return sb.toString();
    }

    /**
     * Parse a given string into a spectrum
     *
     * @param spectrumString given string content
     * @return parsed spectrum
     */
    public static ISpectrum parseSpectrumFromMGFString(String spectrumString) {
        LineNumberReader reader = new LineNumberReader(new StringReader(spectrumString));
        return ParserUtilities.readMGFScan(reader);
    }

    /**
     * convert a cluster to string for output
     *
     * @param cluster given cluster
     * @return string   represents cluster in CGF format
     */
    public static String convertClusterToCGFString(ICluster cluster) {
        StringBuilder sb = new StringBuilder();
        CGFClusterAppender clusterAppender = CGFClusterAppender.INSTANCE;
        clusterAppender.appendCluster(sb, cluster);
        return sb.toString();
    }


    /**
     * Parse a given string into a cluster
     *
     * @param clusterString cluster string in CGF format
     * @return a cluster object
     */
    public static ICluster parseClusterFromCGFString(String clusterString) {
        LineNumberReader rdr = new LineNumberReader((new StringReader(clusterString)));
        return ParserUtilities.readSpectralCluster(rdr, null);
    }

    /**
     * Parse a group of clusters
     * @param clusterString string that represents a group of cluster
     * @return  an array of clusters
     */
    public static ICluster[] parseClustersFromCGFString(String clusterString) {
        LineNumberReader rdr = new LineNumberReader((new StringReader(clusterString)));
        return ParserUtilities.readSpectralCluster(rdr);
    }


    /**
     * convert a cluster to string for output
     *
     * @param cluster given cluster
     * @return string   represents cluster in JSON format
     */
    public static String convertClusterToJSONClusteringString(ICluster cluster) {
        StringBuilder sb = new StringBuilder();
        DotClusterClusterAppender clusterAppender = DotClusterClusterAppender.INSTANCE;
        clusterAppender.appendCluster(sb, cluster);
        return sb.toString();
    }

    /**
     * This method parse the folder containing all the mgf files into a MzBin, ISpectrum. It is important to notice that
     * the current data structure allows that the BinMZKey can contains multiple spectra for the same precursor binned value. For
     * this reason the system returns a MZkey and a List of ISpectrum.
     *
     * @param context   JavaSpark Context
     * @param inputPath Folder that contains all the spectra files (mgf input)
     * @return Map with the BinMZKey corresponding to the precursor Mass and the List of Spectra.
     */
    public static JavaPairRDD parseMGFRDD(JavaSparkContext context, String inputPath) {

        Class inputFormatClass = MGFInputFormat.class;
        Class keyClass         = String.class;
        Class valueClass       = String.class;

        return context.newAPIHadoopFile(
                inputPath,
                inputFormatClass,
                keyClass,
                valueClass,
                context.hadoopConfiguration()
        );
    }
}
