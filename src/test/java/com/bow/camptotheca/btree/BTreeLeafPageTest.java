package com.bow.camptotheca.btree;

import com.bow.camptotheca.BTreeFileEncoder;
import com.bow.camptotheca.BTreeLeafPage;
import com.bow.camptotheca.BTreePageId;
import com.bow.camptotheca.BTreeUtility;
import com.bow.camptotheca.BufferPool;
import com.bow.camptotheca.Database;
import com.bow.camptotheca.DbException;
import com.bow.camptotheca.IntField;
import com.bow.camptotheca.SimpleDbTestBase;
import com.bow.camptotheca.SystemTestUtil;
import com.bow.camptotheca.TestUtil;
import com.bow.camptotheca.TransactionId;
import com.bow.camptotheca.Tuple;
import com.bow.camptotheca.Type;
import com.bow.camptotheca.Utility;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;

import static org.junit.Assert.*;

public class BTreeLeafPageTest {

    private BTreePageId pid;

    public static final int[][] EXAMPLE_VALUES = new int[][] {
        { 1, 2 },
        { 2, 2 }
    };

    public static final ArrayList<Tuple> TUPLES = new ArrayList<Tuple>();

    public static final byte[] PAGE_DATA;

    static {
        for (int[] tuple : EXAMPLE_VALUES) {
            Tuple tup = new Tuple(Utility.getTupleDesc(2));
            for (int i = 0; i < tuple.length; i++) {
                tup.setField(i, new IntField(tuple[i]));
            }
            TUPLES.add(tup);
        }

        // Convert it to a BTreeLeafPage
        try {
            PAGE_DATA = BTreeFileEncoder.convertToLeafPage(TUPLES, BufferPool.getPageSize(), 2,
                    new Type[] { Type.INT_TYPE, Type.INT_TYPE }, 0);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Set up initial resources for each unit test.
     */
    @Before
    public void addTable() throws Exception {
        this.pid = new BTreePageId(-1, -1, BTreePageId.LEAF);
        Database.getCatalog().addTable(new TestUtil.SkeletonFile(-1, Utility.getTupleDesc(2)),
                SystemTestUtil.getUUID());
    }

    /**
     * Unit test for BTreeLeafPage.getParentId()
     */
    @Test
    public void getId() throws Exception {
        BTreeLeafPage page = new BTreeLeafPage(pid, PAGE_DATA, 0);
        assertEquals(new BTreePageId(pid.getTableId(), 0, BTreePageId.ROOT_PTR), page.getParentId());
        assertTrue(page.getLeftSiblingId() == null);
        assertTrue(page.getRightSiblingId() == null);
    }

    /**
     * Unit test for BTreeLeafPage.setParentId()
     */
    @Test
    public void setParentId() throws Exception {
        BTreeLeafPage page = new BTreeLeafPage(pid, PAGE_DATA, 0);
        // 注意叶子节点的父节点不能是BTreePageId.LEAF
        BTreePageId id = new BTreePageId(pid.getTableId(), 1, BTreePageId.INTERNAL);
        page.setParentId(id);
        assertEquals(id, page.getParentId());

    }

    /**
     * Unit test for BTreeLeafPage.setLeftSiblingId()
     */
    @Test
    public void setLeftSiblingId() throws Exception {
        BTreeLeafPage page = new BTreeLeafPage(pid, PAGE_DATA, 0);
        BTreePageId id = new BTreePageId(pid.getTableId(), 1, BTreePageId.LEAF);
        page.setLeftSiblingId(id);
        // 叶子节点的兄弟节点不能是BTreePageId.INTERNAL
        assertEquals(id, page.getLeftSiblingId());
    }

    /**
     * Unit test for BTreeLeafPage.iterator()
     */
    @Test
    public void testIterator() throws Exception {
        // 以第一个字段构建索引
        BTreeLeafPage page = new BTreeLeafPage(pid, PAGE_DATA, 0);
        Iterator<Tuple> it = page.iterator();
        while (it.hasNext()) {
            Tuple tup = it.next();
            System.out.println(tup.getField(0) + " , " + tup.getField(1));
        }
    }

    /**
     * Unit test for BTreeLeafPage.isDirty()
     */
    @Test
    public void testDirty() throws Exception {
        TransactionId tid = new TransactionId();
        BTreeLeafPage page = new BTreeLeafPage(pid, PAGE_DATA, 0);
        page.markDirty(true, tid);
        TransactionId dirtier = page.isDirty();
        assertEquals(true, dirtier != null);
        assertEquals(true, dirtier == tid);

        page.markDirty(false, tid);
        dirtier = page.isDirty();
        assertEquals(false, dirtier != null);
    }

    /**
     * Unit test for BTreeLeafPage.addTuple()
     */
    @Test
    public void addTuple() throws Exception {
        byte[] data = BTreeLeafPage.createEmptyPageData();
        BTreeLeafPage page0 = new BTreeLeafPage(pid, data, 0);

        System.out.println(page0.getNumEmptySlots());
        for (int i = 0; i < TUPLES.size(); i++) {
            page0.insertTuple(TUPLES.get(i));
        }
        System.out.println(page0.getNumEmptySlots());
        TestUtil.printHex(page0.getPageData());

    }

    /**
     * Unit test for BTreeLeafPage.deleteTuple() with false TUPLES
     */
    @Test(expected = DbException.class)
    public void deleteNonexistentTuple() throws Exception {
        BTreeLeafPage page = new BTreeLeafPage(pid, PAGE_DATA, 0);
        page.deleteTuple(BTreeUtility.getBTreeTuple(2, 2));
    }

    /**
     * Unit test for BTreeLeafPage.deleteTuple()
     */
    @Test
    public void deleteTuple() throws Exception {
        BTreeLeafPage page = new BTreeLeafPage(pid, PAGE_DATA, 0);
        int free = page.getNumEmptySlots();

        // first, build a list of the TUPLES on the page.
        Iterator<Tuple> it = page.iterator();
        LinkedList<Tuple> tuples = new LinkedList<Tuple>();
        while (it.hasNext())
            tuples.add(it.next());
        Tuple first = tuples.getFirst();

        // now, delete them one-by-one from both the front and the end.
        int deleted = 0;
        while (tuples.size() > 0) {
            page.deleteTuple(tuples.removeFirst());
            page.deleteTuple(tuples.removeLast());
            deleted += 2;
            assertEquals(free + deleted, page.getNumEmptySlots());
        }

        // now, the page should be empty.
        try {
            page.deleteTuple(first);
            throw new Exception("page should be empty; expected DbException");
        } catch (DbException e) {
            // explicitly ignored
        }
    }

}
