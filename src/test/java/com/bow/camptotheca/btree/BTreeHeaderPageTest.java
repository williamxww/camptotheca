package com.bow.camptotheca.btree;

import com.bow.camptotheca.db.BTreeHeaderPage;
import com.bow.camptotheca.db.BTreePageId;
import com.bow.camptotheca.db.TestUtil;
import com.bow.camptotheca.db.TransactionId;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;


public class BTreeHeaderPageTest {

    private BTreePageId pid;

    public static final byte[] EXAMPLE_DATA = BTreeHeaderPage.createEmptyPageData();

    @Before
    public void setup() throws Exception {
        this.pid = new BTreePageId(-1, -1, BTreePageId.HEADER);
    }

    /**
     * Unit test for BTreeHeaderPage.getId()
     */
    @Test
    public void commonOperation() throws Exception {
        BTreeHeaderPage page = new BTreeHeaderPage(pid, EXAMPLE_DATA);
        assertEquals(pid, page.getId());
        assertTrue(page.getNextPageId() == null);

        // 注意 tableId 和 page Type 需要和父页保持一致
        BTreePageId id = new BTreePageId(pid.getTableId(), 1, BTreePageId.HEADER);
        page.setPrevPageId(id);
        assertEquals(id, page.getPrevPageId());

        // 每页有前一页 后一页两个指针，每个指针都是int类型 (4096-4*2)*8 = 32704
        assertEquals(32704, BTreeHeaderPage.getNumSlots());
    }

    @Test
    public void getEmptySlot() throws Exception {
        BTreeHeaderPage page = new BTreeHeaderPage(pid, EXAMPLE_DATA);
        assertEquals(0, page.getEmptySlot());
        page.init();
        assertEquals(-1, page.getEmptySlot());
        page.markSlotUsed(50, false);
        // 获取第一个空slot的序号
        assertEquals(50, page.getEmptySlot());
    }


    /**
     * Unit test for BTreeHeaderPage.isDirty()
     */
    @Test
    public void testDirty() throws Exception {
        TransactionId tid = new TransactionId();
        BTreeHeaderPage page = new BTreeHeaderPage(pid, EXAMPLE_DATA);
        page.markDirty(true, tid);
        TransactionId dirtier = page.isDirty();
        assertEquals(true, dirtier != null);
        assertEquals(true, dirtier == tid);

        page.markDirty(false, tid);
        dirtier = page.isDirty();
        assertEquals(false, dirtier != null);
    }

    @Test
    public void testGetData() throws IOException {
        BTreeHeaderPage page = new BTreeHeaderPage(pid, EXAMPLE_DATA);
        page.init();//最开始的2个int为0后面其他bit全为1

        //下面几句执行后，指针后的第一个byte变为fc  c: 1100
        page.markSlotUsed(0,false);//将第一个slot改为0，即没有被占用
        page.markSlotUsed(1,false);
        page.markSlotUsed(2,true);
        page.markSlotUsed(3,true);
        TestUtil.printHex(page.getPageData());
    }



}
