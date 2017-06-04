package com.bow.camptotheca.btree;

import com.bow.camptotheca.BTreeFile;
import com.bow.camptotheca.BTreeLeafPage;
import com.bow.camptotheca.BTreePageId;
import com.bow.camptotheca.BTreeRootPtrPage;
import com.bow.camptotheca.BTreeUtility;
import com.bow.camptotheca.Database;
import com.bow.camptotheca.DbFileIterator;
import com.bow.camptotheca.Field;
import com.bow.camptotheca.IndexPredicate;
import com.bow.camptotheca.IntField;
import com.bow.camptotheca.Predicate;
import com.bow.camptotheca.TestUtil;
import com.bow.camptotheca.TransactionId;
import com.bow.camptotheca.Tuple;
import com.bow.camptotheca.TupleDesc;
import com.bow.camptotheca.Utility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


import static org.junit.Assert.*;
public class BTreeFileReadTest{

    private BTreeFile f;

    private TransactionId tid;

    private TupleDesc td;

    /**
     * Set up initial resources for each unit test.
     */
    @Before
    public void setUp() throws Exception {
        Database.reset();
        f = BTreeUtility.createRandomBTreeFile(2, 20, null, null, 0);
        td = Utility.getTupleDesc(2);
        tid = new TransactionId();
    }

    @After
    public void tearDown() throws Exception {
        Database.getBufferPool().transactionComplete(tid);
    }


    /**
     * Unit test for BTreeFile.getTupleDesc()
     */
    @Test
    public void getTupleDesc() throws Exception {
        assertEquals(td, f.getTupleDesc());
    }

    /**
     * Unit test for BTreeFile.numPages()
     */
    @Test
    public void numPages() throws Exception {
        assertEquals(1, f.numPages());
    }

    /**
     * Unit test for BTreeFile.readPage()
     */
    @Test
    public void readPage() throws Exception {
        BTreePageId rootPtrPid = new BTreePageId(f.getId(), 0, BTreePageId.ROOT_PTR);
        BTreeRootPtrPage rootPtr = (BTreeRootPtrPage) f.readPage(rootPtrPid);

        assertEquals(1, rootPtr.getRootId().pageNumber());
        assertEquals(BTreePageId.LEAF, rootPtr.getRootId().pgcateg());

        BTreePageId rootPageId = new BTreePageId(f.getId(), 1, BTreePageId.LEAF);
        BTreeLeafPage rootPage = (BTreeLeafPage) f.readPage(rootPageId);
        TestUtil.printHex(rootPage.getPageData());

    }

    @Test
    public void testIteratorBasic() throws Exception {
        BTreeFile smallFile = BTreeUtility.createRandomBTreeFile(2, 3, null, null, 0);

        DbFileIterator it = smallFile.iterator(tid);

        it.open();
        while (it.hasNext()) {
            Tuple tuple = it.next();
            System.out.println(tuple);
        }
        it.close();
    }


    /**
     * Unit test for BTreeFile.indexIterator()
     */
    @Test
    public void indexIterator() throws Exception {
        BTreeFile twoLeafPageFile = BTreeUtility.createBTreeFile(2, 520, null, null, 0);
        Field f = new IntField(5);

        // greater than
        IndexPredicate ipred = new IndexPredicate(Predicate.Op.GREATER_THAN, f);
        DbFileIterator it = twoLeafPageFile.indexIterator(tid, ipred);
        it.open();
        int count = 0;
        while (it.hasNext()) {
            Tuple t = it.next();
            assertTrue(t.getField(0).compare(Predicate.Op.GREATER_THAN, f));
            count++;
        }
        assertEquals(515, count);
        it.close();
    }

}
