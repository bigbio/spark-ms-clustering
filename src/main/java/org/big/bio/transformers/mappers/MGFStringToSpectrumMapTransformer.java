package org.big.bio.transformers.mappers;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import uk.ac.ebi.pride.spectracluster.io.ParserUtilities;
import uk.ac.ebi.pride.spectracluster.spectrum.ISpectrum;

import java.io.LineNumberReader;
import java.io.StringReader;

/**
 * Convert Text MGF Structure to ISpectrum objects.
 *
 * @author Yasset Perez-Riverol
 */
public class MGFStringToSpectrumMapTransformer implements PairFunction<Tuple2<String, String>, String, ISpectrum> {

    /**
     * Default constructor for the Spectrum Reader.
     */
    public MGFStringToSpectrumMapTransformer() {
    }

    /**
     * Transform the Text from the file into an ISpectrum
     * @param kv Tuple Text, eExt
     * @return the key value pair where the key is the id of the spectrum and the value the ISpectrum object
     * @throws Exception
     */
    @Override
    public Tuple2<String, ISpectrum> call(final Tuple2<String, String> kv) throws Exception {
        LineNumberReader inp = new LineNumberReader(new StringReader(kv._2));
        ISpectrum spectrum = ParserUtilities.readMGFScan(inp);
        return new Tuple2<>(spectrum.getId(), spectrum);
    }
}
