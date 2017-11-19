package org.big.bio.keys;

import uk.ac.ebi.pride.spectracluster.util.MZIntensityUtilities;

/**
 * Some key utilities that can be used to convert from a double key to String and
 * and the way back.
 *
 * @author Yasset Perez-Riverol
 */

public class KeyUtilities {

    /**
     * convert a int into an mz for easy comparison
     *
     * @param mz input
     * @return MZ_RESOLUTION * mz as int
     */
    public static String mzToStringKey(double mz) {
        int peak = MZIntensityUtilities.mzToInt(mz);
        return String.format("%010d", peak);
    }

    /**
     * convert an int into an mz for east comparison
     *
     * @param key input
     * @return MZ_RESOLUTION * mz as int
     */
    public static double keyToMZ(String key) {
        double ret = Integer.parseInt(key); // (double)MZ_RESOLUTION;
        return ret / MZIntensityUtilities.MZ_RESOLUTION;
    }

    /**
     * convert an int into an mz for east comparison
     *
     * @return MZ_RESOLUTION * mz as int
     */
    public static double mzToKey(double precursor) {
        int peak = MZIntensityUtilities.mzToInt(precursor);
        return keyToMZ(String.format("%010d", peak));
    }
}
