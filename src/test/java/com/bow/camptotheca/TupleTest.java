package com.bow.camptotheca;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TupleTest{

    /**
     * Unit test for Tuple.getField() and Tuple.setField()
     */
    @Test
    public void modifyFields() {
        //构造包含两个int字段的Tuple
        TupleDesc td = Utility.getTupleDesc(2);
        Tuple tup = new Tuple(td);

        tup.setField(0, new IntField(-1));
        tup.setField(1, new IntField(0));
        assertEquals(new IntField(-1), tup.getField(0));
        assertEquals(new IntField(0), tup.getField(1));

        tup.setField(0, new IntField(1));
        tup.setField(1, new IntField(37));
        assertEquals(tup.getField(0), new IntField(1));
        assertEquals(tup.getField(1), new IntField(37));
    }


    /**
     * Unit test for Tuple.getRecordId() and Tuple.setRecordId()
     */
    @Test
    public void modifyRecordId() {
        Tuple tup1 = new Tuple(Utility.getTupleDesc(1));
        HeapPageId pid1 = new HeapPageId(0, 0);
        RecordId rid1 = new RecordId(pid1, 0);
        tup1.setRecordId(rid1);

        assertEquals(rid1, tup1.getRecordId());
    }

}
