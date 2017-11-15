package org.big.bio.transformers;

import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;
import uk.ac.ebi.pride.spectracluster.io.ParserUtilities;
import uk.ac.ebi.pride.spectracluster.spectrum.ISpectrum;

import java.io.LineNumberReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Convert Text MGF Structure to ISpectrum objects.
 *
 * @author Yasset Perez-Riverol
 */
public class MGFStringToSpectrumTransformer implements PairFlatMapFunction<Tuple2<String, String>, String, ISpectrum> {

    /**
     * Default constructor for the Spectrum Reader.
     */
    public MGFStringToSpectrumTransformer() {
    }

    /**
     * Transform the Text from the file into an ISpectrum
     * @param kv Tuple Text, eExt
     * @return the key value pair where the key is the id of the spectrum and the value the ISpectrum object
     * @throws Exception
     */
    @Override
    public Iterator<Tuple2<String, ISpectrum>> call(final Tuple2<String, String> kv) throws Exception {
        List<Tuple2<String, ISpectrum>> ret = new ArrayList<>();
        LineNumberReader inp = new LineNumberReader(new StringReader(kv._2.toString()));
        ISpectrum spectrum = ParserUtilities.readMGFScan(inp);
        ret.add(new Tuple2<>(spectrum.getId(), spectrum));
        return ret.iterator();
    }
}
