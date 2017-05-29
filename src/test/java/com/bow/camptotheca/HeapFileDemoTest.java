package com.bow.camptotheca;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.UUID;

/**
 * 此demo描述HeapFile的写和读。HeapFile不支持随机读写。
 * 
 * @author vv
 * @since 2017/5/29.
 */
public class HeapFileDemoTest {

    private TupleDesc tupleDesc;

    private HeapFile heapFile;

    @Before
    public void setup() throws Exception {
        File file = new File("vv.data");
        if (!file.exists()) {
            file.createNewFile();
        }

        Type[] types = new Type[] { Type.INT_TYPE, Type.INT_TYPE };
        tupleDesc = new TupleDesc(types);
        // 创建一个heapFile并在Database的目录中注册
        heapFile = new HeapFile(file, tupleDesc);
        Database.getCatalog().addTable(heapFile, UUID.randomUUID().toString());

    }

    @Test
    public void writeData() throws Exception {

        // 创建page0 写到文件中
        HeapPageId pageId = new HeapPageId(heapFile.getId(), 0);
        HeapPage page = new HeapPage(pageId, HeapPage.createEmptyPageData());
        heapFile.writePage(page);

        // 创建一条记录
        Tuple tuple = new Tuple(tupleDesc);
        tuple.setRecordId(new RecordId(pageId, 0));
        tuple.setField(0, new IntField(0xFF));
        tuple.setField(1, new IntField(0xEE));

        TransactionId tid = new TransactionId();
        heapFile.insertTuple(tid, tuple);

        // commit
        Database.getBufferPool().transactionComplete(tid);
    }

    @Test
    public void scan() throws Exception {
        TransactionId tid = new TransactionId();
        SeqScan scan = new SeqScan(tid, heapFile.getId(), "table");

        // 遍历打印每个tuple
        scan.open();
        if (scan.hasNext()) {
            Tuple tuple = scan.next();
            StringBuilder sb = new StringBuilder("");
            for (int i = 0; i < tuple.getTupleDesc().numFields(); ++i) {
                int value = ((IntField) tuple.getField(i)).getValue();
                sb.append(Integer.toHexString(value)).append(" ");
            }
            System.out.println(sb);
        }

        // scan.rewind();
        scan.close();
        Database.getBufferPool().transactionComplete(tid);
    }
}
