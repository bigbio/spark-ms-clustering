package org.big.bio.transformers.mappers;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.big.bio.clustering.pride.PRIDEClusterDefaultParameters;
import org.big.bio.keys.MZKey;
import scala.Tuple2;
import uk.ac.ebi.pride.spectracluster.cluster.ICluster;
import uk.ac.ebi.pride.spectracluster.spectrum.IPeak;
import uk.ac.ebi.pride.spectracluster.spectrum.ISpectrum;
import uk.ac.ebi.pride.spectracluster.spectrum.Spectrum;
import uk.ac.ebi.pride.spectracluster.util.ClusterUtilities;
import uk.ac.ebi.pride.spectracluster.util.MZIntensityUtilities;
import uk.ac.ebi.pride.spectracluster.util.binner.IWideBinner;
import uk.ac.ebi.pride.spectracluster.util.binner.SizedWideBinner;
import uk.ac.ebi.pride.spectracluster.util.function.Functions;
import uk.ac.ebi.pride.spectracluster.util.function.IFunction;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

/**
 * This code is licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * ==Overview==
 * <p>
 * This class enable the mapping from Spectrum to Cluster it includes some pre-processing
 * steps to remove un-wanted
 * <p>
 * Created by Yasset Perez-Riverol  (ypriverol@gmail.com) on 01/11/2017.
 */
public class SpectrumToInitialClusterMapTransformer implements PairFunction<Tuple2<String, ISpectrum>, MZKey, ICluster> {

    private static final double BIN_OVERLAP = 0;

    private static final float DEFAULT_BIN_WIDTH = 4F;

    private static final boolean OVERFLOW_BINS = true;

    private static final double LOWEST_MZ = 0;

    private IWideBinner binner;

    public static final String WINDOW_SIZE_PROPERTY = "mapper.window_size";

    private IFunction<ISpectrum, ISpectrum> initialFilters;

    /**
     * This Constructor define the parameters to do the transformation from ISpectrum to Cluster.
     * It defines the bin With for all the spectra.
     *
     * @param context Java Spark Context
     */
    public SpectrumToInitialClusterMapTransformer(JavaSparkContext context){

        // Read the Bin from the configuration file.
        double binWidth = context.hadoopConfiguration().getFloat(WINDOW_SIZE_PROPERTY, DEFAULT_BIN_WIDTH);

        binner = new SizedWideBinner(MZIntensityUtilities.HIGHEST_USABLE_MZ, binWidth, LOWEST_MZ, BIN_OVERLAP, OVERFLOW_BINS);

        // Init the filters for the file
        initialFilters = PRIDEClusterDefaultParameters.INITIAL_SPECTRUM_FILTER;

        // Add all the initial filters to the list.
        for (IFunction<ISpectrum, ISpectrum> iSpectrumISpectrumIFunction : PRIDEClusterDefaultParameters.getConfigurableSpectraFilters(context.hadoopConfiguration())) {
            initialFilters = Functions.join(initialFilters, iSpectrumISpectrumIFunction);
        }

    }

    /**
     * Transform the Spectrum into ICluster with the corresponding
     * @param spectrumTuple Spectrum Tuple.
     * @return MZKey - ICluster Maps.
     * @throws Exception IO Error iterating the Spectrum file.
     */
    @Override
    public Tuple2<MZKey, ICluster> call(Tuple2<String, ISpectrum> spectrumTuple) throws Exception {

        ISpectrum spectrum = spectrumTuple._2();
        float precursorMz = spectrum.getPrecursorMz();

        if (precursorMz < MZIntensityUtilities.HIGHEST_USABLE_MZ) {

            // do initial filtering (ie. precursor removal, impossible high peaks, etc.)
            ISpectrum filteredSpectrum = initialFilters.apply(spectrum);

            // Normalized Peaks
            ISpectrum normalisedSpectrum = normaliseSpectrum(filteredSpectrum);

            // Spectrum Filter Peaks
            normalisedSpectrum = new Spectrum(filteredSpectrum, PRIDEClusterDefaultParameters.HIGHEST_N_PEAK_INTENSITY_FILTER.apply(normalisedSpectrum.getPeaks()));

            // generate a new cluster
            ICluster cluster = ClusterUtilities.asCluster(normalisedSpectrum);

            // get the bin(s)
            int[] bins = binner.asBins(cluster.getPrecursorMz());

            // make sure the spectrum is only placed in a single bin since overlaps cannot happen in this config
            if (bins.length != 1) {
                throw new InterruptedException("This implementation only works if now overlap is set during binning.");
            }

            // This is really important because, here is when the cluster ID Is generated. We can explored the
            // idea in the future to generate the ID based on the Spectrum Cluster Peak List.
            cluster.setId(UUID.randomUUID().toString());

            // output cluster
            MZKey mzKey = new MZKey(precursorMz);

            return new Tuple2<>(mzKey, cluster);
        }
        return null;
    }

    /**
     * This method normalize an Spectrum
     * @param filteredSpectrum spectrum to be Normalized
     * @return Normalized Spectrum
     */
    private ISpectrum normaliseSpectrum(ISpectrum filteredSpectrum) {
        List<IPeak> normalizedPeaks = PRIDEClusterDefaultParameters.DEFAULT_INTENSITY_NORMALIZER.normalizePeaks(filteredSpectrum.getPeaks());
        return new Spectrum(filteredSpectrum, normalizedPeaks);
    }
}
