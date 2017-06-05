package com.bow.camptotheca.btree;

import com.bow.camptotheca.db.BTreeEntry;
import com.bow.camptotheca.db.BTreeFile;
import com.bow.camptotheca.db.BTreeFileEncoder;
import com.bow.camptotheca.db.BTreeHeaderPage;
import com.bow.camptotheca.db.BTreeInternalPage;
import com.bow.camptotheca.db.BTreeLeafPage;
import com.bow.camptotheca.db.BTreePageId;
import com.bow.camptotheca.db.BTreeRootPtrPage;
import com.bow.camptotheca.db.BufferPool;
import com.bow.camptotheca.db.Database;
import com.bow.camptotheca.db.DbFileIterator;
import com.bow.camptotheca.db.HeapFile;
import com.bow.camptotheca.db.IntField;
import com.bow.camptotheca.db.SystemTestUtil;
import com.bow.camptotheca.db.TestUtil;
import com.bow.camptotheca.db.TransactionId;
import com.bow.camptotheca.db.Tuple;
import com.bow.camptotheca.db.TupleDesc;
import com.bow.camptotheca.db.Type;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

/**
 * @author vv
 * @since 2017/5/29.
 */
public class BTreeFileDemoTest {

    public static final byte[] EXAMPLE_DATA = BTreeHeaderPage.createEmptyPageData();

    @Before
    public void setup() {
        Type[] types = new Type[] { Type.INT_TYPE, Type.INT_TYPE };
        TupleDesc tupleDesc = new TupleDesc(types);
        Database.getCatalog().addTable(new TestUtil.SkeletonFile(-1, tupleDesc), SystemTestUtil.getUUID());
    }

    @Test
    public void test() throws Exception {
        int tableId = -1;
        BTreePageId pid = new BTreePageId(tableId, 0, BTreePageId.HEADER);
        BTreeHeaderPage page = new BTreeHeaderPage(pid, EXAMPLE_DATA);

        // 注意BTreePageId只能为HEADER
        BTreePageId prevId = new BTreePageId(tableId, 1, BTreePageId.HEADER);
        page.setPrevPageId(prevId);

        BTreePageId nextId = new BTreePageId(tableId, 2, BTreePageId.HEADER);
        page.setNextPageId(nextId);

        System.out.println(BTreeHeaderPage.getNumSlots());

        assertEquals(0, page.getEmptySlot());
        page.init();
        assertEquals(-1, page.getEmptySlot());
        page.markSlotUsed(50, false);
        assertEquals(50, page.getEmptySlot());
    }

    @Test
    public void create() throws Exception {
        Type[] types = new Type[] { Type.INT_TYPE, Type.INT_TYPE };
        TupleDesc tupleDesc = new TupleDesc(types);

        // 以第0列为索引
        int keyField = 0;

        File indexFile = new File("table.index");
        File dataFile = new File("table.data");
        // if (!indexFile.exists()) {
        // indexFile.createNewFile();
        // }

        // 创建一个heapFile并在Database的目录中注册
        HeapFile heapFile = new HeapFile(dataFile, tupleDesc);
        Database.getCatalog().addTable(heapFile, UUID.randomUUID().toString());

        // 从heapFile中读取所有记录(Tuple)并对其排序
        TransactionId tid = new TransactionId();
        ArrayList<Tuple> tupleList = new ArrayList();
        DbFileIterator it = Database.getCatalog().getDatabaseFile(heapFile.getId()).iterator(tid);
        it.open();
        while (it.hasNext()) {
            Tuple tup = it.next();
            tupleList.add(tup);
        }
        it.close();
        Collections.sort(tupleList, new BTreeFileEncoder.TupleComparator(keyField));


        // 构造BTreeFile
        BTreeFile bTreeFile = new BTreeFile(indexFile, keyField, tupleDesc);
        Database.getCatalog().addTable(bTreeFile, UUID.randomUUID().toString());

        int pageSize = BufferPool.getPageSize();
        int tableId = bTreeFile.getId();


        //构造叶子
        BTreePageId leftLeafPageId = new BTreePageId(tableId, 2, BTreePageId.LEAF);
        // 将tuple转换为字节
        byte[] leafPageBytes = BTreeFileEncoder.convertToLeafPage(tupleList, pageSize, types.length, types, keyField);
        BTreeLeafPage leftLeafPage = new BTreeLeafPage(leftLeafPageId, leafPageBytes, keyField);

        // 构造内部索引节点
        BTreePageId internalPageId = new BTreePageId(tableId, 1, BTreePageId.INTERNAL);
        ArrayList<BTreeEntry> entryList = new ArrayList<BTreeEntry>();

        BTreeEntry entry = new BTreeEntry(new IntField(10), leftLeafPageId, null);
        entryList.add(entry);
        byte[] internalPageBytes = BTreeFileEncoder.convertToInternalPage(entryList,pageSize, Type.INT_TYPE,
                BTreePageId.LEAF);
        BTreeInternalPage parent = new BTreeInternalPage(internalPageId, internalPageBytes, keyField);
        leftLeafPage.setParentId(internalPageId);


        //构造根
        BTreePageId rootPageId = new BTreePageId(tableId, 0, BTreePageId.ROOT_PTR);
        BTreeRootPtrPage rootPtrPage = new BTreeRootPtrPage(rootPageId, BTreeRootPtrPage.createEmptyPageData());

        bTreeFile.writePage(rootPtrPage);
        bTreeFile.writePage(leftLeafPage);

    }
}
