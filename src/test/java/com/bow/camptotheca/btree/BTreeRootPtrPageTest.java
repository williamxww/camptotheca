package com.bow.camptotheca.btree;

import com.bow.camptotheca.db.BTreeFileEncoder;
import com.bow.camptotheca.db.BTreePageId;
import com.bow.camptotheca.db.BTreeRootPtrPage;
import com.bow.camptotheca.db.Database;
import com.bow.camptotheca.db.SystemTestUtil;
import com.bow.camptotheca.db.TestUtil;
import com.bow.camptotheca.db.TransactionId;
import com.bow.camptotheca.db.Utility;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class BTreeRootPtrPageTest {

    private BTreePageId pid;

    public static final byte[] EXAMPLE_DATA;
    static {

        try {
            // 第1页为root页，页类型为BTreePageId.LEAF，第一个header页的页号
            EXAMPLE_DATA = BTreeFileEncoder.convertToRootPtrPage(1, BTreePageId.LEAF, 2);
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
        this.pid = new BTreePageId(-1, 0, BTreePageId.ROOT_PTR);
        Database.getCatalog().addTable(new TestUtil.SkeletonFile(-1, Utility.getTupleDesc(2)),
                SystemTestUtil.getUUID());
    }

    /**
     * Unit test for BTreeRootPtrPage.setRootId()
     */
    @Test
    public void setRootId() throws Exception {
        BTreeRootPtrPage ptrPage = new BTreeRootPtrPage(pid, EXAMPLE_DATA);

        // 获取root页的page id
        assertEquals(new BTreePageId(pid.getTableId(), 1, BTreePageId.LEAF), ptrPage.getRootId());
        BTreePageId id = new BTreePageId(pid.getTableId(), 1, BTreePageId.INTERNAL);
        // root id不能为BTreePageId.ROOT_PTR类型
        ptrPage.setRootId(id);
        assertEquals(id, ptrPage.getRootId());

        // header id
        assertEquals(new BTreePageId(pid.getTableId(), 2, BTreePageId.HEADER), ptrPage.getHeaderId());
        BTreePageId headerId = new BTreePageId(pid.getTableId(), 3, BTreePageId.HEADER);
        // header id 不能为BTreePageId.ROOT_PTR类型
        ptrPage.setHeaderId(headerId);
        assertEquals(headerId, ptrPage.getHeaderId());

    }

    /**
     * Unit test for BTreeRootPtrPage.isDirty()
     */
    @Test
    public void testDirty() throws Exception {
        TransactionId tid = new TransactionId();
        BTreeRootPtrPage page = new BTreeRootPtrPage(pid, EXAMPLE_DATA);
        page.markDirty(true, tid);
        TransactionId dirtier = page.isDirty();
        assertEquals(true, dirtier != null);
        assertEquals(true, dirtier == tid);

        page.markDirty(false, tid);
        dirtier = page.isDirty();
        assertEquals(false, dirtier != null);
    }

    @Test
    public void print() throws IOException {
        BTreeRootPtrPage page = new BTreeRootPtrPage(pid, EXAMPLE_DATA);
        TestUtil.printHex(page.getPageData());
    }

}
