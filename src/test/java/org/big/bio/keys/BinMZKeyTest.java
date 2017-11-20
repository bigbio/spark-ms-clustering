package org.big.bio.keys;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * This code is licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * ==Overview==
 * <p>
 * This class test, will test the BinMZ key for the Precursor mass.
 * <p>
 * Created by Yasset Perez-Riverol (ypriverol@gmail.com) on 30/10/2017.
 */
public class BinMZKeyTest {


    private BinMZKey binMZKey;

    @Before
    public void setUp() throws Exception {
        binMZKey = new BinMZKey(11, 120.50);
    }

    @Test
    public void testGetBin() throws Exception {
        assertEquals(11, binMZKey.getBin());
    }

    @Test
    public void testGetPrecursorMZ() throws Exception {
        assertEquals(120.50, binMZKey.getPrecursorMZ(), 0.00001);
    }

    @Test
    public void testToString() throws Exception {
        assertEquals("000011:0000120500", binMZKey.toString());
    }

    @Test
    public void testEquals() throws Exception {
        assertEquals(binMZKey, new BinMZKey(11, 120.50));
    }

    @Test
    public void testHashCode() throws Exception {
        assertEquals(-800812544, binMZKey.hashCode());
    }


//    @Test
//    public void testGetPartitionHash() throws Exception {
//        assertEquals(1420005920, binMZKey.getPartitionHash());
//    }

}