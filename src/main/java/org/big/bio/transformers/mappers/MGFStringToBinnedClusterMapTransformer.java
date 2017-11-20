package org.big.bio.transformers.mappers;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.big.bio.clustering.pride.PRIDEClusterDefaultParameters;
import org.big.bio.keys.BinMZKey;
import scala.Tuple2;
import uk.ac.ebi.pride.spectracluster.cluster.ICluster;
import uk.ac.ebi.pride.spectracluster.io.ParserUtilities;
import uk.ac.ebi.pride.spectracluster.spectrum.IPeak;
import uk.ac.ebi.pride.spectracluster.spectrum.ISpectrum;
import uk.ac.ebi.pride.spectracluster.spectrum.Spectrum;
import uk.ac.ebi.pride.spectracluster.util.ClusterUtilities;
import uk.ac.ebi.pride.spectracluster.util.MZIntensityUtilities;
import uk.ac.ebi.pride.spectracluster.util.binner.IWideBinner;
import uk.ac.ebi.pride.spectracluster.util.binner.SizedWideBinner;
import uk.ac.ebi.pride.spectracluster.util.function.Functions;
import uk.ac.ebi.pride.spectracluster.util.function.IFunction;

import java.io.LineNumberReader;
import java.io.StringReader;
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
 * This class converts every Spectrum into a single spectrum clusters. This is used in the previous
 * Cluster Algorithm as starting point for clustering.
 *
 * Read Paper here <a href="http://www.nature.com/nmeth/journal/v13/n8/full/nmeth.3902.html">Griss J. and Perez-Riverol Y. </a>
 *
 * <p>
 * Created by Yasset Perez-Riverol (ypriverol@gmail.com) on 31/10/2017.
 */
public class MGFStringToBinnedClusterMapTransformer implements PairFunction<Tuple2<String, String>, BinMZKey, ICluster> {

    private IWideBinner binner;
    private IFunction<ISpectrum, ISpectrum> initialFilters;


    /**
     * This class Binner specific precursor mass for all clusters. Still not aggregation has been performed at this point.
     *
     * @param context JavaSparkContext.
     */
    public MGFStringToBinnedClusterMapTransformer(JavaSparkContext context, String binWidthName) {

        float binWidth = context.hadoopConfiguration().getFloat(binWidthName, PRIDEClusterDefaultParameters.DEFAULT_BINNER_WIDTH);

        binner = new SizedWideBinner( MZIntensityUtilities.HIGHEST_USABLE_MZ,  binWidth,  0,  0,  true);

        // Init the filters for the file
        initialFilters = PRIDEClusterDefaultParameters.INITIAL_SPECTRUM_FILTER;

        // Add all the initial filters to the list.
        for (IFunction<ISpectrum, ISpectrum> iSpectrumISpectrumIFunction : PRIDEClusterDefaultParameters.getConfigurableSpectraFilters(context.hadoopConfiguration())) {
            initialFilters = Functions.join(initialFilters, iSpectrumISpectrumIFunction);
        }

    }

    @Override
    public Tuple2<BinMZKey, ICluster> call(final Tuple2<String, String> kv) throws Exception {

        LineNumberReader inp = new LineNumberReader(new StringReader(kv._2));

        ISpectrum spectrum = ParserUtilities.readMGFScan(inp);

        if (spectrum.getPrecursorMz() < MZIntensityUtilities.HIGHEST_USABLE_MZ) {

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

            BinMZKey binMZKey = new BinMZKey(bins[0], cluster.getPrecursorMz());

            return new Tuple2<>(binMZKey, cluster);
        }
        return null;
    }

    /**
     * Return the current Binner for clustering the MZ Precursor mass
     * @return IWideBinner
     */
    public IWideBinner getBinner() {
        return binner;
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
