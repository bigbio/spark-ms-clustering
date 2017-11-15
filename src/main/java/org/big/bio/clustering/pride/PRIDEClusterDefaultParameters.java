package org.big.bio.clustering.pride;


import org.apache.hadoop.conf.Configuration;
import org.big.bio.core.IConfigurationParameters;
import uk.ac.ebi.pride.spectracluster.normalizer.IIntensityNormalizer;
import uk.ac.ebi.pride.spectracluster.similarity.ISimilarityChecker;
import uk.ac.ebi.pride.spectracluster.spectrum.IPeak;
import uk.ac.ebi.pride.spectracluster.spectrum.ISpectrum;
import uk.ac.ebi.pride.spectracluster.util.Defaults;
import uk.ac.ebi.pride.spectracluster.util.MZIntensityUtilities;
import uk.ac.ebi.pride.spectracluster.util.NumberUtilities;
import uk.ac.ebi.pride.spectracluster.util.binner.IWideBinner;
import uk.ac.ebi.pride.spectracluster.util.binner.SizedWideBinner;
import uk.ac.ebi.pride.spectracluster.util.function.Functions;
import uk.ac.ebi.pride.spectracluster.util.function.IFunction;
import uk.ac.ebi.pride.spectracluster.util.function.peak.FractionTICPeakFunction;
import uk.ac.ebi.pride.spectracluster.util.function.peak.HighestNPeakFunction;
import uk.ac.ebi.pride.spectracluster.util.function.spectrum.RemoveImpossiblyHighPeaksFunction;
import uk.ac.ebi.pride.spectracluster.util.function.spectrum.RemoveIonContaminantsPeaksFunction;
import uk.ac.ebi.pride.spectracluster.util.function.spectrum.RemovePrecursorPeaksFunction;
import uk.ac.ebi.pride.spectracluster.util.function.spectrum.RemoveWindowPeaksFunction;


import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Default configurations for running pride cluster algorithm, when the algorithm is run, this parameters are used if they are not provided
 * by the user in the configuration file.
 *
 *
 * @author Yasset Perez-Riverol
 *
 */
public class PRIDEClusterDefaultParameters implements IConfigurationParameters {

    /** Default PRIDE Cluster Algorithm values  **/

    // Major clustering sliding window is this
    public static final double DEFAULT_MAJOR_PEAK_MZ_WINDOW  = 4.0;

    // window to merge to spectrum in one cluster.
    public static final double DEFAULT_SPECTRUM_MERGE_WINDOW = 0.5;

    // the number of major peaks that would be consider in the first iteration of clustering
    public static final int DEFAULT_MAJOR_PEAK_COUNT = 5;

    // Enable the peak filter
    public static final boolean DEFAULT_ENABLE_COMPARISON_PEAK_FILTER = true;

    // Number of highest intensity peaks that would be consider for the clustering.
    public static final int DEFAULT_INITIAL_HIGHEST_PEAK_FILTER = 150;

    //Number of comparisons to be performed in the algorithm
    public static final int DEFAULT_MIN_NUMBER_COMPARISONS  = 10000;

    /**
     * If this option is > 0, a new clustering engine is being
     * created if the total number of clusters is reached
     */
    public static final int DEFAULT_MAXIMUM_NUMBER_OF_CLUSTERS = 0;

    // Binning sizes
    private static final double NARRROW_BIN_WIDTH = 1; // 0.15; //0.005; // 0.3;
    private static final double NARRROW_BIN_OVERLAP = 0; // 0.03; //0.002; // 0.1;

    // Default WIDE MZ Binner
    public static final IWideBinner DEFAULT_WIDE_MZ_BINNER = new SizedWideBinner(MZIntensityUtilities.HIGHEST_USABLE_MZ,
            NARRROW_BIN_WIDTH, MZIntensityUtilities.LOWEST_USABLE_MZ,
            NARRROW_BIN_OVERLAP);

    // This is the default checker for the similarity CombinedFisherIntesityTest
    public static final String DEFAULT_SIMILARITY_CHECKER_CLASS = "uk.ac.ebi.pride.spectracluster.similarity.CombinedFisherIntensityTest";

    // Start threshold of the PRIDE Cluster algorithm.
    public static final Float DEFAULT_START_THRESHOLD = 0.999F;

    // End threshold of the PRIDE Cluster algorithm.
    public static final Float DEFAULT_END_THRESHOLD  = 0.99F;

    // The default export file format is CDF, if this variable is set to false, them the data will be exported to JSON
    public static final boolean DEFAULT_EXPORT_FILE_FORMAT_CDF = true;

    // Number of rounds to perform the clustering
    public static final int DEFAULT_ROUNDS_CLUSTERING = 4;

    // Filter immonium
    public static final boolean DEFAULT_FILTER_SPECTRA_IMMONIUM = true ;

    // Filter mz150 spectra
    public static final boolean DEFAULT_FILTER_SPECTRA_MZ150 = true;

    // Filter mz200 spectra
    public static final boolean DEFAULT_FILTER_SPECTRA_MZ200 = true;

    public static final float DEFAULT_INIT_BINNER_WINDOW = 0.05F;

    public static final float DEFAULT_BINNER_WIDTH = 4F;

    /** Label of each Default property of PRIDE cluster algorithm  **/

    public static final String INIT_CURRENT_BINNER_WINDOW_PROPERTY = "pride.cluster.init.mapper.window.size";

    public static final String BINNER_WINDOW_PROPERTY = "pride.cluster.mapper.window.size";

    // Number of Peaks to be compare on each Spectra comparison.
    public static final String NUMBER_COMPARED_PEAKS_PROPERTY  = "pride.cluster.number.compared.peaks";


    // Fragment ION Tolerance that would be use to define if two MZ values are equal.
    public static final String FRAGMENT_ION_TOLERANCE_PROPERTY = "pride.cluster.similarity.fragment.tolerance";

    // Retain similarity threshold to keep the spectra withing the same cluster.
    public static final String RETAIN_THRESHOLD_PROPERTY       = "pride.cluster.retain.threshold";

    // Cluster similarity Threshold.
    public static final String SIMILARITY_THRESHOLD_PROPERTY   = "pride.cluster.similarity.threshold";

    // Todo: We need to define what is this.
    public static final String SPECTRUM_MERGE_WINDOW_PROPERTY  = "pride.cluster.spectrum.merge.window";

    // Major Peaks windows define the threshold between the major peak and all the rest peaks in the spectrum.
    public static final String MAJOR_PEAK_WINDOW_PROPERTY      = "pride.cluster.major.clustering.window";

    //Clustering minimum num,ber of comparisons by cluster.
    public static final String CDF_MIN_NUMBER_COMPARISONS_PROPERTY    = "pride.cluster.cdf.min_number_comparisons";

    // Todo: We need to define what is this.
    public static final String ENABLE_COMPARISON_PEAK_FILTER_PROPERTY = "enable.comparison.clustering.filter";

    // Todo: We need to define what is this.
    public static final String INITIAL_HIGHEST_PEAK_FILTER_PROPERTY   = "pride.cluster.initial.highest.clustering.filter";

    public static final String MAXIMUM_NUMBER_CLUSTERS_PROPERTY       = "pride.cluster.maximum_number_clusters";

    // Check the similarity checker method.
    public static final String SIMILARITY_CHECKER_PROPERTY            = "pride.cluster.similarity.checker";

    // PRIDE Cluster consensus min of peaks.
    public static final String CONSENSUS_SPEC_MIN_PEAKS_PROPERTY      = "pride.cluster.consensus_min_peaks";

    // PRIDE Cluster Major Peak Count first iteration.
    public static final String MAJOR_PEAK_COUNT_PROPERTY              = "pride.cluster.major.peak.count";

    //PRIDE Cluster start threshold accuracy.
    public static final String CLUSTER_START_THRESHOLD_PROPERTY       = "pride.cluster.start.threshold";

    //PRIDE Cluster end threshold accuracy
    public static final String CLUSTER_END_THRESHOLD_PROPERTY         = "pride.cluster.end.threshold";

    //This define the type of format that will be used to export the data in cluster
    public static final String CLUSTER_EXPORT_FORMAT_CDF_PROPERTY     = "pride.cluster.export.cdf.clustering";

    // Number of rounds
    public static final String CLUSTERING_ROUNDS_PROPERTY             = "pride.cluster.round.clustering";

    // Filter Spectra by immonium
    public static final String CLUSTERING_FILTER_SPECTRA_IMMONIUM_PROPERTY       = "pride.cluster.spectra.filter.immonium_ions";

    //Filter Spectra MZ Windows
    public static final String CLUSTERING_FILTER_SPECTRA_MZ150_PROPERTY = "pride.cluster.spectra.filer.mz_150";
    public static final String CLUSTERING_FILTER_SPECTRA_MZ200_PROPERTY = "pride.cluster.spectra.filter.mz_200";

    /**
     * Get Default properties for PRIDE Cluster algorithm. Some of this properties are defined in
     * Spectra Cluster sequential algorithm https://github.com/spectra-cluster/spectra-cluster
     *
     * @return Default Properties of PRIDE Cluster algorithm
     */

    @Override
    public Properties getProperties() {
        Properties properties = new Properties();

        // Set the default properties, most of these properties are defined in the sequential algorithm.
        properties.setProperty(NUMBER_COMPARED_PEAKS_PROPERTY, String.valueOf(Defaults.getNumberComparedPeaks()));
        properties.setProperty(FRAGMENT_ION_TOLERANCE_PROPERTY, NumberUtilities.formatDouble(Defaults.getFragmentIonTolerance(), 3));
        properties.setProperty(SIMILARITY_THRESHOLD_PROPERTY, NumberUtilities.formatDouble(Defaults.getSimilarityThreshold(), 3));
        properties.setProperty(RETAIN_THRESHOLD_PROPERTY, NumberUtilities.formatDouble(Defaults.getRetainThreshold(), 3));
        properties.setProperty(MAJOR_PEAK_WINDOW_PROPERTY, NumberUtilities.formatDouble(PRIDEClusterDefaultParameters.DEFAULT_MAJOR_PEAK_MZ_WINDOW, 3));
        properties.setProperty(SPECTRUM_MERGE_WINDOW_PROPERTY, NumberUtilities.formatDouble(PRIDEClusterDefaultParameters.DEFAULT_SPECTRUM_MERGE_WINDOW, 3));
        properties.setProperty(CONSENSUS_SPEC_MIN_PEAKS_PROPERTY, String.valueOf(Defaults.getDefaultConsensusMinPeaks()));
        properties.setProperty(CDF_MIN_NUMBER_COMPARISONS_PROPERTY, String.valueOf(PRIDEClusterDefaultParameters.DEFAULT_MIN_NUMBER_COMPARISONS));
        properties.setProperty(CONSENSUS_SPEC_MIN_PEAKS_PROPERTY, String.valueOf(Defaults.DEFAULT_CONSENSUS_MIN_PEAKS));
        properties.setProperty(SIMILARITY_CHECKER_PROPERTY, PRIDEClusterDefaultParameters.DEFAULT_SIMILARITY_CHECKER_CLASS);
        properties.setProperty(ENABLE_COMPARISON_PEAK_FILTER_PROPERTY, Boolean.toString(DEFAULT_ENABLE_COMPARISON_PEAK_FILTER));
        properties.setProperty(MAXIMUM_NUMBER_CLUSTERS_PROPERTY, String.valueOf(DEFAULT_MAXIMUM_NUMBER_OF_CLUSTERS));
        properties.setProperty(INITIAL_HIGHEST_PEAK_FILTER_PROPERTY, String.valueOf(DEFAULT_INITIAL_HIGHEST_PEAK_FILTER));
        properties.setProperty(MAJOR_PEAK_COUNT_PROPERTY, String.valueOf(DEFAULT_MAJOR_PEAK_COUNT));
        properties.setProperty(CLUSTER_START_THRESHOLD_PROPERTY, String.valueOf(DEFAULT_START_THRESHOLD));
        properties.setProperty(CLUSTER_END_THRESHOLD_PROPERTY, String.valueOf(DEFAULT_END_THRESHOLD));
        properties.setProperty(CLUSTER_EXPORT_FORMAT_CDF_PROPERTY, String.valueOf(DEFAULT_EXPORT_FILE_FORMAT_CDF));
        properties.setProperty(CLUSTERING_ROUNDS_PROPERTY, String.valueOf(DEFAULT_ROUNDS_CLUSTERING));
        properties.setProperty(CLUSTERING_FILTER_SPECTRA_IMMONIUM_PROPERTY, String.valueOf(DEFAULT_FILTER_SPECTRA_IMMONIUM));
        properties.setProperty(CLUSTERING_FILTER_SPECTRA_MZ150_PROPERTY, String.valueOf(DEFAULT_FILTER_SPECTRA_MZ150));
        properties.setProperty(CLUSTERING_FILTER_SPECTRA_MZ200_PROPERTY, String.valueOf(DEFAULT_FILTER_SPECTRA_MZ200));
        properties.setProperty(INIT_CURRENT_BINNER_WINDOW_PROPERTY, String.valueOf(DEFAULT_INIT_BINNER_WINDOW));
        properties.setProperty(BINNER_WINDOW_PROPERTY, String.valueOf(DEFAULT_BINNER_WIDTH));

        return properties;
    }


    /**
     * The similarity checker allow to determinate the similarity between to spectrum
     * @param configuration Haddop configuration
     * @return return the Similarity Checker class
     */
    public static ISimilarityChecker getSimilarityCheckerFromConfiguration(Configuration configuration) {
        Class similarityCheckerClass = configuration.getClass(PRIDEClusterDefaultParameters.SIMILARITY_CHECKER_PROPERTY, Defaults.getDefaultSimilarityChecker().getClass(), ISimilarityChecker.class);
        ISimilarityChecker similarityChecker;
        try {
            similarityChecker = (ISimilarityChecker) similarityCheckerClass.newInstance();
        }
        catch (Exception e) {
            // throw an IllegalStateException for now
            throw new IllegalStateException(e);
        }

        return similarityChecker;
    }

    /**
     * Get all the Spectra filters configured by the Users
     * @param configuration Hadoop configuration
     * @return List of IFunction
     */
    public static List<IFunction<ISpectrum, ISpectrum>> getConfigurableSpectraFilters(Configuration configuration){
        List<IFunction<ISpectrum, ISpectrum>> filters = new ArrayList<>();

        /** Add all the filters at spectra level **/

        // Remove the immonium
        if(configuration.get(CLUSTERING_FILTER_SPECTRA_IMMONIUM_PROPERTY) == null || Boolean.parseBoolean(configuration.get(CLUSTERING_FILTER_SPECTRA_IMMONIUM_PROPERTY)))
            filters.add(new RemoveIonContaminantsPeaksFunction(0.1F));

        // Remove the mz150 Windows
        if(configuration.get(CLUSTERING_FILTER_SPECTRA_MZ150_PROPERTY) == null || Boolean.parseBoolean(configuration.get(CLUSTERING_FILTER_SPECTRA_MZ150_PROPERTY)))
            filters.add( new RemoveWindowPeaksFunction(150.0F, Float.MAX_VALUE));

        // Remove the mz200 Windows
        if(configuration.get(CLUSTERING_FILTER_SPECTRA_MZ200_PROPERTY) == null || Boolean.parseBoolean(configuration.get(CLUSTERING_FILTER_SPECTRA_MZ200_PROPERTY)))
            filters.add( new RemoveWindowPeaksFunction(200.0F, Float.MAX_VALUE));

        return filters;

    }

    /** This filter is applied to all spectra as soon as they are loaded from file.
     * These filters are not configurable, they are applied to all spectra loaded into the algorithm.
     */

    // Remove the Impossible High Peaks and the Precursor Peak
    public static final IFunction<ISpectrum,ISpectrum> INITIAL_SPECTRUM_FILTER = Functions
            .join(new RemoveImpossiblyHighPeaksFunction(),
                    new RemovePrecursorPeaksFunction(Defaults.getFragmentIonTolerance()),
                            new RemoveIonContaminantsPeaksFunction(Defaults.getFragmentIonTolerance()));


    // This filter is applied to all spectra when loaded from the file - after the initial spectrum filter.
    public static final IFunction<List<IPeak>, List<IPeak>> HIGHEST_N_PEAK_INTENSITY_FILTER = new HighestNPeakFunction(70);

    /**
     * Return the default Intensity normalizer
     */
    public static final IIntensityNormalizer DEFAULT_INTENSITY_NORMALIZER = Defaults.getDefaultIntensityNormalizer();

    /**
     * The filter applied to every spectrum when performing the actual comparison. This
     * filter does not affect the spectra from which the consensus spectrum is build.
     * Only if fast mode is available.
     * Todo: Ask to Johannes the influence in the algorithm.
     */
    public static final IFunction<List<IPeak>, List<IPeak>> DEFAULT_COMPARISON_FILTER_FUNCTION = new FractionTICPeakFunction(0.5f, 20);

}
