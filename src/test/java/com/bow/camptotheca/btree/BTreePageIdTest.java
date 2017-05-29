package com.bow.camptotheca.btree;

import com.bow.camptotheca.BTreePageId;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class BTreePageIdTest  {

    private BTreePageId rootPtrId;

    private BTreePageId internalId;

    private BTreePageId leafId;

    private BTreePageId headerId;

    @Before
    public void createPid() {
        // 1个table文件多个page页
        rootPtrId = new BTreePageId(1, 0, BTreePageId.ROOT_PTR);
        internalId = new BTreePageId(1, 1, BTreePageId.INTERNAL);
        leafId = new BTreePageId(1, 2, BTreePageId.LEAF);
        headerId = new BTreePageId(1, 3, BTreePageId.HEADER);
    }

    /**
     * Unit test for BTreePageId.getTableId()
     */
    @Test
    public void getTableId() {
        assertEquals(1, rootPtrId.getTableId());
        assertEquals(1, internalId.getTableId());
        assertEquals(1, leafId.getTableId());
        assertEquals(1, headerId.getTableId());
    }

    /**
     * Unit test for BTreePageId.pageno()
     */
    @Test
    public void pageno() {
        assertEquals(0, rootPtrId.pageNumber());
        assertEquals(1, internalId.pageNumber());
        assertEquals(2, leafId.pageNumber());
        assertEquals(3, headerId.pageNumber());
    }

}
