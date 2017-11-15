package org.big.bio.keys;


/**
 * Key for the MZ Precursor mass, When a map is converted into a key-value pair where the key is the
 * mz value (first randomization of the data by mz values ) can be done with this key. The BinMZ is needed because
 * when two mass values are compare they are done using a Bin Size rather than the exacted mass. Bin is defined at he very beginning
 * of the algorithm.
 *
 * @author Yasset Perez-Riverol
 *
 */
public class BinMZKey implements IKeyable<BinMZKey> {

    private final int bin;
    private final double precursorMZ;
    private final String binKey;
    private final String precursorMZKey;

    public BinMZKey(final int pBin, final double pPrecursorMZ) {
        bin = pBin;
        precursorMZ = pPrecursorMZ;
        binKey = String.format("%06d", getBin());
        precursorMZKey = KeyUtilities.mzToKey(getPrecursorMZ());
    }

    public BinMZKey(String str) {
        String[] split = str.split(":");
        bin = Integer.parseInt(split[0]);
        precursorMZ = KeyUtilities.keyToMZ(split[1]);
        binKey = String.format("%06d", getBin());
        precursorMZKey = KeyUtilities.mzToKey(getPrecursorMZ());
    }

    /**
     * MZ_RESOLUTION * peakMZ
     *
     * @return Bin
     */
    public int getBin() {
        return bin;
    }

    /**
     * MZ_RESOLUTION * getPrecursorMZ
     *
     * @return Precursor MZ
     */
    public double getPrecursorMZ() {
        return precursorMZ;
    }

    @Override
    public String toString() {
        return binKey + ":" + precursorMZKey;
    }


   @Override
    public boolean equals(final Object o) {
        return o != null && getClass() == o.getClass() && bin == ((BinMZKey)o).bin;
    }

//    @Override
//    public boolean equals(final Object o) {
//        return o != null && getClass() == o.getClass() && toString().equals(o.toString());
//    }


    @Override
    public int hashCode() {
        return toString().hashCode();
    }


    /**
     * sort by string works
     *
     * @param binMZKey binMzKey is a binned MZ Precursor mass
     * @return compare int.
     */
    @Override
    public int compareTo(final BinMZKey binMZKey) {
        if(binMZKey != null)
            return toString().compareTo(binMZKey.toString());
        return 1;
    }

//    @Override
//    public int compareTo(final BinMZKey binMZKey){
//        if((precursorMZ + bin) > (binMZKey.precursorMZ - bin))
//            return -1;
//        else if((binMZKey.precursorMZ + bin) < (precursorMZ - bin))
//            return 1;
//        return 0;
//    }

    /**
     * here is an int that a partitioner would use
     *
     * @return Partition Hash.
     */
    public int getPartitionHash() {
        return Math.abs(binKey.hashCode());
    }
}
