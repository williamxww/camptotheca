package com.bow.camptotheca.btree;

import com.bow.camptotheca.BTreeEntry;
import com.bow.camptotheca.BTreeFile;
import com.bow.camptotheca.BTreeFileEncoder;
import com.bow.camptotheca.BTreeHeaderPage;
import com.bow.camptotheca.BTreeInternalPage;
import com.bow.camptotheca.BTreeLeafPage;
import com.bow.camptotheca.BTreePageId;
import com.bow.camptotheca.BTreeRootPtrPage;
import com.bow.camptotheca.BTreeUtility;
import com.bow.camptotheca.BufferPool;
import com.bow.camptotheca.Database;
import com.bow.camptotheca.DbFileIterator;
import com.bow.camptotheca.HeapFile;
import com.bow.camptotheca.IntField;
import com.bow.camptotheca.SystemTestUtil;
import com.bow.camptotheca.TestUtil;
import com.bow.camptotheca.TransactionId;
import com.bow.camptotheca.Tuple;
import com.bow.camptotheca.TupleDesc;
import com.bow.camptotheca.Type;
import com.bow.camptotheca.Utility;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
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
        ArrayList<Tuple> tuples = new ArrayList();
        DbFileIterator it = Database.getCatalog().getDatabaseFile(heapFile.getId()).iterator(tid);
        it.open();
        while (it.hasNext()) {
            Tuple tup = it.next();
            tuples.add(tup);
        }
        it.close();
        Collections.sort(tuples, new BTreeFileEncoder.TupleComparator(keyField));


        // 将排序过的tuple加入到b+树中
        BTreeFile bTreeFile = new BTreeFile(indexFile, keyField, tupleDesc);
        Database.getCatalog().addTable(bTreeFile, UUID.randomUUID().toString());



        int pageSize = BufferPool.getPageSize();
        int tableId = bTreeFile.getId();

        //构造根
        BTreePageId rootPageId = new BTreePageId(tableId, 0, BTreePageId.ROOT_PTR);
        BTreeRootPtrPage rootPtrPage = new BTreeRootPtrPage(rootPageId, BTreeRootPtrPage.createEmptyPageData());

        //构造一个叶子
        BTreePageId leftPageId = new BTreePageId(tableId, 2, BTreePageId.LEAF);
        // 将tuple转换为字节
        byte[] leafPageBytes = BTreeFileEncoder.convertToLeafPage(tuples, pageSize, types.length, types, keyField);
        BTreeLeafPage leftPage = new BTreeLeafPage(leftPageId, leafPageBytes, keyField);

        // 构造父节点
        BTreePageId parentId = new BTreePageId(tableId, 1, BTreePageId.INTERNAL);
        ArrayList<BTreeEntry> entries = new ArrayList<BTreeEntry>();

        //entry 的key为左page中最大的记录的key吗？
        BTreeEntry entry = new BTreeEntry(new IntField(10), leftPageId, null);
        entries.add(entry);
        byte[] internalPageBytes = BTreeFileEncoder.convertToInternalPage(entries,pageSize, Type.INT_TYPE,
                BTreePageId.LEAF);
        BTreeInternalPage parent = new BTreeInternalPage(parentId, internalPageBytes, keyField);
        leftPage.setParentId(parentId);

        bTreeFile.writePage(rootPtrPage);
        bTreeFile.writePage(leftPage);

    }
}
