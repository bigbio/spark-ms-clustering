package org.big.bio.transformers;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.big.bio.clustering.pride.PRIDEClusterDefaultParameters;
import org.big.bio.keys.BinMZKey;
import org.big.bio.keys.MZKey;
import org.big.bio.utils.SparkUtil;
import scala.Tuple2;
import uk.ac.ebi.pride.spectracluster.cluster.ICluster;
import uk.ac.ebi.pride.spectracluster.util.MZIntensityUtilities;
import uk.ac.ebi.pride.spectracluster.util.binner.IWideBinner;
import uk.ac.ebi.pride.spectracluster.util.binner.SizedWideBinner;

import java.util.ArrayList;
import java.util.Iterator;
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
 * This class performed Binner in the Precursor Mass.
 * <p>
 * @author Yasset Perez-Riverol (ypriverol@gmail.com) on 01/11/2017.
 */

public class PrecursorBinnerTransformer implements PairFlatMapFunction<Tuple2<MZKey, ICluster>, BinMZKey, ICluster> {

    private IWideBinner binner;

    private static final Logger LOGGER = Logger.getLogger(PrecursorBinnerTransformer.class);


    /**
     * This class Binner specific precursor mass for all clusters. Still not aggregation has been performed at this point.
     *
     * @param context JavaSparkContext.
     */
    public PrecursorBinnerTransformer(JavaSparkContext context, String binWidthName) {

        float binWidth = context.hadoopConfiguration().getFloat(binWidthName, PRIDEClusterDefaultParameters.DEFAULT_BINNER_WIDTH);

        binner = new SizedWideBinner( MZIntensityUtilities.HIGHEST_USABLE_MZ,  binWidth,  0,  0,  true);

        boolean offsetBins = context.hadoopConfiguration().getBoolean("pride.cluster.offset.bins", false);
        if (offsetBins) {
            binner = (IWideBinner) binner.offSetHalf();
        }
    }

    @Override
    public Iterator<Tuple2<BinMZKey, ICluster>> call(Tuple2<MZKey, ICluster> tupleCluster) throws Exception {

        List<Tuple2<BinMZKey, ICluster>> ret = new ArrayList<>();
        IWideBinner binner = getBinner();
        ICluster cluster = tupleCluster._2();
        float precursorMz = cluster.getPrecursorMz();
        int[] bins = binner.asBins(precursorMz);


        // must only be in one bin
        if (bins.length > 1) {
                throw new InterruptedException("Multiple bins found for " + String.valueOf(precursorMz));
        } else if (bins.length != 0) {
            BinMZKey binMZKey = new BinMZKey(bins[0], precursorMz);
            ret.add(new Tuple2<>(binMZKey, cluster));
        }
        return ret.iterator();
    }

    /**
     * Return the current Binner for clustering the MZ Precursor mass
     * @return IWideBinner
     */
    public IWideBinner getBinner() {
        return binner;
    }
}
