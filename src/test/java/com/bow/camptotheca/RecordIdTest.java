package com.bow.camptotheca;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.junit.Before;
import org.junit.Test;


public class RecordIdTest  {

    private static RecordId recordId1;

    private static RecordId recordId2;

    private static RecordId recordId3;

    private static RecordId recordId4;

    @Before
    public void createPids() {
        HeapPageId hpid1 = new HeapPageId(-1, 2);
        HeapPageId hpid2 = new HeapPageId(-1, 2);
        HeapPageId hpid3 = new HeapPageId(-2, 2);
        recordId1 = new RecordId(hpid1, 3);
        recordId2 = new RecordId(hpid2, 3);
        recordId3 = new RecordId(hpid1, 4);
        recordId4 = new RecordId(hpid3, 3);

    }

    /**
     * Unit test for RecordId.getPageId()
     */
    @Test
    public void getPageId() {
        HeapPageId hpid = new HeapPageId(-1, 2);
        assertEquals(hpid, recordId1.getPageId());

    }

    /**
     * Unit test for RecordId.tupleno()
     */
    @Test
    public void tupleno() {
        assertEquals(3, recordId1.tupleno());
    }

    /**
     * Unit test for RecordId.equals()
     */
    @Test
    public void equals() {
        assertEquals(recordId1, recordId2);
        assertEquals(recordId2, recordId1);
        assertFalse(recordId1.equals(recordId3));
        assertFalse(recordId3.equals(recordId1));
        assertFalse(recordId2.equals(recordId4));
        assertFalse(recordId4.equals(recordId2));
    }

    /**
     * Unit test for RecordId.hashCode()
     */
    @Test
    public void hCode() {
        assertEquals(recordId1.hashCode(), recordId2.hashCode());
    }

}
