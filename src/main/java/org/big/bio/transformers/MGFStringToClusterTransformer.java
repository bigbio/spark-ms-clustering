package org.big.bio.transformers;

import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;
import uk.ac.ebi.pride.spectracluster.cluster.ICluster;
import uk.ac.ebi.pride.spectracluster.io.ParserUtilities;
import uk.ac.ebi.pride.spectracluster.spectrum.ISpectrum;
import uk.ac.ebi.pride.spectracluster.util.ClusterUtilities;

import java.io.LineNumberReader;
import java.io.StringReader;
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
 * This class converts every Spectrum into a single spectrum clusters. This is used in the previous
 * Cluster Algorithm as starting point for clustering.
 *
 * Read Paper here <a href="http://www.nature.com/nmeth/journal/v13/n8/full/nmeth.3902.html">Griss J. and Perez-Riverol Y. </a>
 *
 * <p>
 * Created by Yasset Perez-Riverol (ypriverol@gmail.com) on 31/10/2017.
 */
public class MGFStringToClusterTransformer implements PairFlatMapFunction<Tuple2<String, String>, String, ICluster> {

    @Override
    public Iterator<Tuple2<String, ICluster>> call(final Tuple2<String, String> kv) throws Exception {
        List<Tuple2<String, ICluster>> ret = new ArrayList<>();
        LineNumberReader inp = new LineNumberReader(new StringReader(kv._2));
        ISpectrum spectrum = ParserUtilities.readMGFScan(inp);
        ICluster cluster = ClusterUtilities.asCluster(spectrum);
        ret.add(new Tuple2<>(cluster.getId(), cluster));
        return ret.iterator();
    }
}
