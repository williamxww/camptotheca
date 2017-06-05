package com.bow.camptotheca.btree;

import com.bow.camptotheca.db.BTreeEntry;
import com.bow.camptotheca.db.BTreeFileEncoder;
import com.bow.camptotheca.db.BTreeInternalPage;
import com.bow.camptotheca.db.BTreePageId;
import com.bow.camptotheca.db.BTreeUtility;
import com.bow.camptotheca.db.BufferPool;
import com.bow.camptotheca.db.Database;
import com.bow.camptotheca.db.DbException;
import com.bow.camptotheca.db.IntField;
import com.bow.camptotheca.db.SystemTestUtil;
import com.bow.camptotheca.db.TestUtil;
import com.bow.camptotheca.db.TransactionId;
import com.bow.camptotheca.db.Type;
import com.bow.camptotheca.db.Utility;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;

import static org.junit.Assert.*;


public class BTreeInternalPageTest {


    private BTreePageId pid;

    // these entries have been carefully chosen to be valid entries when
    // inserted in order. Be careful if you change them!
    public static final int[][] EXAMPLE_VALUES = new int[][] {
            { 2, 6350, 4 },
            { 4, 9086, 5 },
            { 5, 17197, 7 },
            { 7, 22064, 9 },
            { 9, 22189, 10 },
            { 10, 28617, 11 },
            { 11, 31933, 13 },
            { 13, 33549, 14 },
            { 14, 34784, 15 },
            { 15, 42878, 17 },
            { 17, 45569, 19 },
            { 19, 56462, 20 },
            { 20, 62778, 21 },
            { 15, 42812, 16 },
            { 2, 3596, 3 },
            { 6, 17876, 7 },
            { 1, 1468, 2 },
            { 11, 29402, 12 },
            { 18, 51440, 19 },
            { 7, 19209, 8 } };

    public static final byte[] PAGE_DATA;

    static {
        // Build the input table
        ArrayList<BTreeEntry> entryList = new ArrayList<BTreeEntry>();
        for (int[] value : EXAMPLE_VALUES) {
            BTreePageId leftChild = new BTreePageId(-1, value[0], BTreePageId.LEAF);
            BTreePageId rightChild = new BTreePageId(-1, value[2], BTreePageId.LEAF);
            BTreeEntry entry = new BTreeEntry(new IntField(value[1]), leftChild, rightChild);
            entryList.add(entry);
        }

        // Convert it to a BTreeInternalPage
        try {
            PAGE_DATA = BTreeFileEncoder.convertToInternalPage(entryList, BufferPool.getPageSize(), Type.INT_TYPE,
                    BTreePageId.LEAF);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Set up initial resources for each unit test.
     */
    @Before
    public void addTable() throws Exception {
        Database.reset();
        this.pid = new BTreePageId(-1, -1, BTreePageId.INTERNAL);
        //向数据库注册此page对应的数据库文件的tupleDesc是2个Int型的
        Database.getCatalog().addTable(new TestUtil.SkeletonFile(-1, Utility.getTupleDesc(2)),
                SystemTestUtil.getUUID());
    }


    /**
     * Unit test for BTreeInternalPage.getParentId()
     */
    @Test
    public void parentId() throws Exception {
        BTreeInternalPage page = new BTreeInternalPage(pid, PAGE_DATA, 0);
        assertEquals(new BTreePageId(pid.getTableId(), 0, BTreePageId.ROOT_PTR), page.getParentId());
        // internal Page 的父节点不能是BTreePageId.LEAF，并且父节点的tableId也应该与子节点保持一致
        BTreePageId id = new BTreePageId(pid.getTableId(), 1, BTreePageId.INTERNAL);
        page.setParentId(id);
        assertEquals(id, page.getParentId());
    }


    /**
     * Unit test for BTreeInternalPage.iterator()
     */
    @Test
    public void testIterator() throws Exception {
        BTreeInternalPage page = new BTreeInternalPage(pid, PAGE_DATA, 0);
        Iterator<BTreeEntry> it = page.iterator();

        while (it.hasNext()) {
            BTreeEntry e = it.next();
            System.out.println(e);
        }
    }

    /**
     * Unit test for BTreeInternalPage.reverseIterator()
     */
    @Test
    public void testReverseIterator() throws Exception {
        BTreeInternalPage page = new BTreeInternalPage(pid, PAGE_DATA, 0);
        Iterator<BTreeEntry> it = page.reverseIterator();

        while (it.hasNext()) {
            BTreeEntry e = it.next();
            System.out.println(e.getKey());
        }
    }

    /**
     * Unit test for BTreeInternalPage.getNumEmptySlots()
     */
    @Test
    public void getNumEmptySlots() throws Exception {
        BTreeInternalPage page = new BTreeInternalPage(pid, PAGE_DATA, 0);
        assertEquals(483, page.getNumEmptySlots());
    }

    /**
     * Unit test for BTreeInternalPage.isSlotUsed()
     */
    @Test
    public void getSlot() throws Exception {
        BTreeInternalPage page = new BTreeInternalPage(pid, PAGE_DATA, 0);

        // assuming the first slot is used for the extra child pointer
        for (int i = 0; i < 21; ++i)
            assertTrue(page.isSlotUsed(i));

        for (int i = 21; i < 504; ++i)
            assertFalse(page.isSlotUsed(i));
    }

    /**
     * Unit test for BTreeInternalPage.isDirty()
     */
    @Test
    public void testDirty() throws Exception {
        TransactionId tid = new TransactionId();
        BTreeInternalPage page = new BTreeInternalPage(pid, PAGE_DATA, 0);
        page.markDirty(true, tid);
        TransactionId dirtier = page.isDirty();
        assertEquals(true, dirtier != null);
        assertEquals(true, dirtier == tid);

        page.markDirty(false, tid);
        dirtier = page.isDirty();
        assertEquals(false, dirtier != null);
    }

    /**
     * Unit test for BTreeInternalPage.addEntry()
     */
    @Test
    public void addEntry() throws Exception {
        // create a blank page
        byte[] data = BTreeInternalPage.createEmptyPageData();
        BTreeInternalPage page = new BTreeInternalPage(pid, data, 0);
        // 除了每一个bit代表一个entry状态外，还有一个bit代表多出的那个索引的状态
        int entryNum = page.getMaxEntries();
        int headerBit = entryNum + 1;
        int headerByte = headerBit % 8 == 0 ? headerBit / 8 : headerBit / 8 + 1;
        System.out.println("header size(Byte) " + headerByte + " entry Num " + entryNum); // 63
        // 503

        // insert entries into the page
        int[][] values = { { 1, 0xAA, 2 }, { 2, 0xAB, 3 }, { 3, 0xAC, 4 } };
        for (int[] entry : values) {
            BTreePageId leftChild = new BTreePageId(pid.getTableId(), entry[0], BTreePageId.LEAF);
            BTreePageId rightChild = new BTreePageId(pid.getTableId(), entry[2], BTreePageId.LEAF);
            BTreeEntry e = new BTreeEntry(new IntField(entry[1]), leftChild, rightChild);
            page.insertEntry(e);
        }

        TestUtil.printHex(page.getPageData());
        // 从上面可以看出序列化后依次为4个字节的父指针，1个字节的子节点类型，63字节的header, 503*4字节的keyField,
        // 504*4字节的子节点指针。
    }

    /**
     * Unit test for BTreeInternalPage.deleteEntry() with false entries
     */
    @Test(expected = DbException.class)
    public void deleteNonexistentEntry() throws Exception {
        BTreeInternalPage page = new BTreeInternalPage(pid, PAGE_DATA, 0);
        page.deleteKeyAndRightChild(BTreeUtility.getBTreeEntry(2));
    }

    /**
     * Unit test for BTreeInternalPage.deleteEntry()
     */
    @Test
    public void deleteEntry() throws Exception {
        BTreeInternalPage page = new BTreeInternalPage(pid, PAGE_DATA, 0);
        int free = page.getNumEmptySlots();

        // first, build a list of the entries on the page.
        Iterator<BTreeEntry> it = page.iterator();
        LinkedList<BTreeEntry> entries = new LinkedList<BTreeEntry>();
        while (it.hasNext())
            entries.add(it.next());
        BTreeEntry first = entries.getFirst();

        // now, delete them one-by-one from both the front and the end.
        int deleted = 0;
        while (entries.size() > 0) {
            page.deleteKeyAndRightChild(entries.removeFirst());
            page.deleteKeyAndRightChild(entries.removeLast());
            deleted += 2;
            assertEquals(free + deleted, page.getNumEmptySlots());
        }

        // now, the page should be empty.
        try {
            page.deleteKeyAndRightChild(first);
            throw new Exception("page should be empty; expected DbException");
        } catch (DbException e) {
            // explicitly ignored
        }
    }

}
