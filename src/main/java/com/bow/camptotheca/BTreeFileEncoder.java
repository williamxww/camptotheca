package com.bow.camptotheca;

import java.io.*;
import java.util.*;

import com.bow.camptotheca.Predicate.Op;

/**
 * BTreeFileEncoder reads a comma delimited text file and converts it to pages
 * of binary data in the appropriate format for simpledb B+ tree pages.
 */

public class BTreeFileEncoder {

    /**
     * Encode the file using the BTreeFile's Insert method.
     * 
     * @param tuples - list of tuples to add to the file
     * @param hFile - the file to temporarily store the data as a heap file on
     *        disk
     * @param bFile - the file on disk to back the resulting BTreeFile
     * @param keyField - the index of the key field for this B+ tree
     * @param numFields - the number of fields in each tuple
     * @return the BTreeFile
     */
    public static BTreeFile convert(ArrayList<ArrayList<Integer>> tuples, File hFile, File bFile, int keyField,
            int numFields) throws IOException {
        File tempInput = File.createTempFile("tempTable", ".txt");
        tempInput.deleteOnExit();
        BufferedWriter bw = new BufferedWriter(new FileWriter(tempInput));
        for (ArrayList<Integer> tuple : tuples) {
            int writtenFields = 0;
            for (Integer field : tuple) {
                writtenFields++;
                if (writtenFields > numFields) {
                    bw.close();
                    throw new RuntimeException(
                            "Tuple has more than " + numFields + " fields: (" + Utility.listToString(tuple) + ")");
                }
                bw.write(String.valueOf(field));
                if (writtenFields < numFields) {
                    bw.write(',');
                }
            }
            bw.write('\n');
        }
        bw.close();
        return convert(tempInput, hFile, bFile, keyField, numFields);
    }

    /**
     * Encode the file using the BTreeFile's Insert method.
     * 
     * @param inFile - the raw text file containing the tuples
     * @param hFile - the file to temporarily store the data as a heap file on
     *        disk
     * @param bFile - the file on disk to back the resulting BTreeFile
     * @param keyField - the index of the key field for this B+ tree
     * @param numFields - the number of fields in each tuple
     * @return the BTreeFile
     */
    public static BTreeFile convert(File inFile, File hFile, File bFile, int keyField, int numFields)
            throws IOException {
        // convert the inFile to HeapFile first.
        HeapFileEncoder.convert(inFile, hFile, BufferPool.getPageSize(), numFields);
        HeapFile heapf = Utility.openHeapFile(numFields, hFile);

        // add the heap file to B+ tree file
        BTreeFile bf = BTreeUtility.openBTreeFile(numFields, bFile, keyField);

        try {
            TransactionId tid = new TransactionId();
            DbFileIterator it = Database.getCatalog().getDatabaseFile(heapf.getId()).iterator(tid);
            it.open();
            int count = 0;
            Transaction t = new Transaction();
            while (it.hasNext()) {
                Tuple tup = it.next();
                Database.getBufferPool().insertTuple(t.getId(), bf.getId(), tup);
                count++;
                if (count >= 40) {
                    Database.getBufferPool().flushAllPages();
                    count = 0;
                }
                t.commit();
                t = new Transaction();
            }
            it.close();
        } catch (TransactionAbortedException te) {
            te.printStackTrace();
            return bf;
        } catch (DbException e) {
            e.printStackTrace();
            return bf;
        } catch (IOException e) {
            e.printStackTrace();
            return bf;
        }

        try {
            Database.getBufferPool().flushAllPages();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return bf;

    }

    /**
     * comparator to sort Tuples by key field
     */
    public static class TupleComparator implements Comparator<Tuple> {
        private int keyField;

        /**
         * Construct a TupleComparator
         * 
         * @param keyField - the index of the field the tuples are keyed on
         */
        public TupleComparator(int keyField) {
            this.keyField = keyField;
        }

        /**
         * Compare two tuples based on their key field
         * 
         * @return -1 if t1 < t2, 1 if t1 > t2, 0 if t1 == t2
         */
        public int compare(Tuple t1, Tuple t2) {
            int cmp = 0;
            if (t1.getField(keyField).compare(Op.LESS_THAN, t2.getField(keyField))) {
                cmp = -1;
            } else if (t1.getField(keyField).compare(Op.GREATER_THAN, t2.getField(keyField))) {
                cmp = 1;
            }
            return cmp;
        }
    }

    /**
     * Faster method to encode the B+ tree file
     * 
     * @param tuples - list of tuples to add to the file
     * @param hFile - the file to temporarily store the data as a heap file on
     *        disk
     * @param bFile - the file on disk to back the resulting BTreeFile
     * @param npagebytes - number of bytes per page
     * @param numFields - number of fields per tuple
     * @param typeAr - array containing the types of the tuples
     * @param fieldSeparator - character separating fields in the raw data file
     * @param keyField - the field of the tuples the B+ tree will be keyed on
     * @return the BTreeFile
     */
    public static BTreeFile convert(ArrayList<ArrayList<Integer>> tuples, File hFile, File bFile, int npagebytes,
            int numFields, Type[] typeAr, char fieldSeparator, int keyField)
            throws IOException, DbException, TransactionAbortedException {
        File tempInput = File.createTempFile("tempTable", ".txt");
        tempInput.deleteOnExit();
        BufferedWriter bw = new BufferedWriter(new FileWriter(tempInput));
        for (ArrayList<Integer> tuple : tuples) {
            int writtenFields = 0;
            for (Integer field : tuple) {
                writtenFields++;
                if (writtenFields > numFields) {
                    bw.close();
                    throw new RuntimeException(
                            "Tuple has more than " + numFields + " fields: (" + Utility.listToString(tuple) + ")");
                }
                bw.write(String.valueOf(field));
                if (writtenFields < numFields) {
                    bw.write(',');
                }
            }
            bw.write('\n');
        }
        bw.close();
        return convert(tempInput, hFile, bFile, npagebytes, numFields, typeAr, fieldSeparator, keyField);
    }

    /**
     * Faster method to encode the B+ tree file
     * 
     * @param inFile - the file containing the raw data
     * @param hFile - the data file for the HeapFile to be used as an
     *        intermediate conversion step
     * @param bFile - the data file for the BTreeFile
     * @param pageSize - number of bytes per page
     * @param numFields - number of fields per tuple
     * @param typeAry - array containing the types of the tuples
     * @param fieldSeparator - character separating fields in the raw data file
     * @param keyField - the field of the tuples the B+ tree will be keyed on
     * @return the B+ tree file
     * @throws IOException
     * @throws DbException
     * @throws TransactionAbortedException
     */
    public static BTreeFile convert(File inFile, File hFile, File bFile, int pageSize, int numFields, Type[] typeAry,
            char fieldSeparator, int keyField) throws IOException, DbException, TransactionAbortedException {
        // convert the inFile to HeapFile first.
        HeapFileEncoder.convert(inFile, hFile, BufferPool.getPageSize(), numFields);
        HeapFile heapf = Utility.openHeapFile(numFields, hFile);

        // read all the tuples from the heap file and sort them on the keyField
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();
        TransactionId tid = new TransactionId();
        DbFileIterator it = Database.getCatalog().getDatabaseFile(heapf.getId()).iterator(tid);
        it.open();
        while (it.hasNext()) {
            Tuple tup = it.next();
            tuples.add(tup);
        }
        it.close();
        Collections.sort(tuples, new TupleComparator(keyField));

        // add the tuples to B+ tree file
        BTreeFile bTreeFile = BTreeUtility.openBTreeFile(numFields, bFile, keyField);
        Type keyType = typeAry[keyField];
        int tableId = bTreeFile.getId();

        // 一条记录的大小
        int recordBytes = 0;
        for (int i = 0; i < numFields; i++) {
            recordBytes += typeAry[i].getLen();
        }

        //叶子节点有3个指针，左右兄弟和父亲
        int leafPointerBytes = 3 * BTreeLeafPage.INDEX_SIZE;
        // 注意每条记录还会有一个bit记录slot的使用状态
        int maxRecordNum = (pageSize * 8 - leafPointerBytes * 8) / (recordBytes * 8 + 1);

        // 每个entry的大小
        int entryBytes = keyType.getLen() + BTreeInternalPage.INDEX_SIZE;
        //除了一批entry外，索引页有一个父指针，额外多出的一个子节点指针，
        int internalPointerBytes = 2 * BTreeLeafPage.INDEX_SIZE + 1;
        int maxEntryNum = (pageSize * 8 - internalPointerBytes * 8 - 1) / (entryBytes * 8 + 1);

        ArrayList<ArrayList<BTreeEntry>> levelEntryList = new ArrayList<ArrayList<BTreeEntry>>();

        // 创建空白的root指针页
        bTreeFile.writePage(new BTreeRootPtrPage(BTreeRootPtrPage.getId(tableId), BTreeRootPtrPage.createEmptyPageData()));

        // next iterate through all the tuples and write out leaf pages
        // and internal pages as they fill up.
        // We wait until we have two full pages of tuples before writing out the
        // first page
        // so that we will not end up with any pages containing less than
        // nrecords/2 tuples (unless it's the only page)
        ArrayList<Tuple> page1 = new ArrayList<Tuple>();
        ArrayList<Tuple> page2 = new ArrayList<Tuple>();
        BTreePageId leftSiblingId = null;
        for (Tuple tup : tuples) {
            if (page1.size() < maxRecordNum) {
                page1.add(tup);
            } else if (page2.size() < maxRecordNum) {
                page2.add(tup);
            } else {
                // 2页都满了就输出第一页到文件中
                byte[] leafPageBytes = convertToLeafPage(page1, pageSize, numFields, typeAry, keyField);
                BTreePageId pageId = new BTreePageId(tableId, bTreeFile.numPages() + 1, BTreePageId.LEAF);
                BTreeLeafPage leafPage = new BTreeLeafPage(pageId, leafPageBytes, keyField);
                leafPage.setLeftSiblingId(leftSiblingId);
                bTreeFile.writePage(leafPage);
                leftSiblingId = pageId;

                // update the parent by "copying up" the next key
                BTreeEntry copyUpEntry = new BTreeEntry(page2.get(0).getField(keyField), pageId, null);
                updateEntries(levelEntryList, bTreeFile, copyUpEntry, 0, maxEntryNum, pageSize, keyType, tableId, keyField);

                page1 = page2;
                page2 = new ArrayList<Tuple>();
                page2.add(tup);
            }
        }

        // now we need to deal with the end cases. There are two options:
        // 1. We have less than or equal to a full page of records. Because of
        // the way the code
        // was written above, we know this must be the only page
        // 2. We have somewhere between one and two pages of records remaining.
        // For case (1), we write out the page
        // For case (2), we divide the remaining records equally between the
        // last two pages,
        // write them out, and update the parent's child pointers.
        BTreePageId lastPid = null;
        if (page2.size() == 0) {
            // write out a page of records - this is the root page
            byte[] lastPageBytes = convertToLeafPage(page1, pageSize, numFields, typeAry, keyField);
            lastPid = new BTreePageId(tableId, bTreeFile.numPages() + 1, BTreePageId.LEAF);
            BTreeLeafPage lastPage = new BTreeLeafPage(lastPid, lastPageBytes, keyField);
            lastPage.setLeftSiblingId(leftSiblingId);
            bTreeFile.writePage(lastPage);
        } else {
            // split the remaining tuples in half
            int remainingTuples = page1.size() + page2.size();
            ArrayList<Tuple> secondToLastPg = new ArrayList<Tuple>();
            ArrayList<Tuple> lastPg = new ArrayList<Tuple>();
            secondToLastPg.addAll(page1.subList(0, remainingTuples / 2));
            lastPg.addAll(page1.subList(remainingTuples / 2, page1.size()));
            lastPg.addAll(page2);

            // write out the last two pages of records
            byte[] secondToLastPageBytes = convertToLeafPage(secondToLastPg, pageSize, numFields, typeAry, keyField);
            BTreePageId secondToLastPid = new BTreePageId(tableId, bTreeFile.numPages() + 1, BTreePageId.LEAF);
            BTreeLeafPage secondToLastPage = new BTreeLeafPage(secondToLastPid, secondToLastPageBytes, keyField);
            secondToLastPage.setLeftSiblingId(leftSiblingId);
            bTreeFile.writePage(secondToLastPage);

            byte[] lastPageBytes = convertToLeafPage(lastPg, pageSize, numFields, typeAry, keyField);
            lastPid = new BTreePageId(tableId, bTreeFile.numPages() + 1, BTreePageId.LEAF);
            BTreeLeafPage lastPage = new BTreeLeafPage(lastPid, lastPageBytes, keyField);
            lastPage.setLeftSiblingId(secondToLastPid);
            bTreeFile.writePage(lastPage);

            // update the parent by "copying up" the next key
            BTreeEntry copyUpEntry = new BTreeEntry(lastPg.get(0).getField(keyField), secondToLastPid, lastPid);
            updateEntries(levelEntryList, bTreeFile, copyUpEntry, 0, maxEntryNum, pageSize, keyType, tableId, keyField);
        }

        // Write out the remaining internal pages
        cleanUpEntries(levelEntryList, bTreeFile, maxEntryNum, pageSize, keyType, tableId, keyField);

        // update the root pointer to point to the last page of the file
        int root = bTreeFile.numPages();
        int rootCategory = (root > 1 ? BTreePageId.INTERNAL : BTreePageId.LEAF);
        byte[] rootPtrBytes = convertToRootPtrPage(root, rootCategory, 0);
        bTreeFile.writePage(new BTreeRootPtrPage(BTreeRootPtrPage.getId(tableId), rootPtrBytes));

        // set all the parent and sibling pointers
        setParents(bTreeFile, new BTreePageId(tableId, root, rootCategory), BTreeRootPtrPage.getId(tableId));
        setRightSiblingPtrs(bTreeFile, lastPid, null);

        Database.resetBufferPool(BufferPool.DEFAULT_PAGES);
        return bTreeFile;
    }

    /**
     * Set all the right sibling pointers by following the left sibling pointers
     * 
     * @param bf - the BTreeFile
     * @param pid - the id of the page to update with the right sibling pointer
     * @param rightSiblingId - the id of the page's right sibling
     * @throws IOException
     * @throws DbException
     */
    private static void setRightSiblingPtrs(BTreeFile bf, BTreePageId pid, BTreePageId rightSiblingId)
            throws IOException, DbException {
        BTreeLeafPage page = (BTreeLeafPage) bf.readPage(pid);
        page.setRightSiblingId(rightSiblingId);
        BTreePageId leftSiblingId = page.getLeftSiblingId();
        bf.writePage(page);
        if (leftSiblingId != null) {
            setRightSiblingPtrs(bf, leftSiblingId, page.getId());
        }
    }

    /**
     * Recursive function to set all the parent pointers
     * 
     * @param bf - the BTreeFile
     * @param pid - id of the page to update with the parent pointer
     * @param parent - the id of the page's parent
     * @throws IOException
     * @throws DbException
     */
    private static void setParents(BTreeFile bf, BTreePageId pid, BTreePageId parent) throws IOException, DbException {
        if (pid.pgcateg() == BTreePageId.INTERNAL) {
            BTreeInternalPage page = (BTreeInternalPage) bf.readPage(pid);
            page.setParentId(parent);

            Iterator<BTreeEntry> it = page.iterator();
            BTreeEntry e = null;
            while (it.hasNext()) {
                e = it.next();
                setParents(bf, e.getLeftChild(), pid);
            }
            if (e != null) {
                setParents(bf, e.getRightChild(), pid);
            }
            bf.writePage(page);
        } else { // pid.pgcateg() == BTreePageId.LEAF
            BTreeLeafPage page = (BTreeLeafPage) bf.readPage(pid);
            page.setParentId(parent);
            bf.writePage(page);
        }
    }

    /**
     * Write out any remaining entries and update the parent pointers.
     * 
     * @param entries - the list of remaining entries
     * @param bf - the BTreeFile
     * @param nentries - number of entries per page
     * @param npagebytes - number of bytes per page
     * @param keyType - the type of the key field
     * @param tableid - the table id of this BTreeFile
     * @param keyField - the index of the key field
     * @throws IOException
     */
    private static void cleanUpEntries(ArrayList<ArrayList<BTreeEntry>> entries, BTreeFile bf, int nentries,
            int npagebytes, Type keyType, int tableid, int keyField) throws IOException {
        // As with the leaf pages, there are two options:
        // 1. We have less than or equal to a full page of entries. Because of
        // the way the code
        // was written, we know this must be the root page
        // 2. We have somewhere between one and two pages of entries remaining.
        // For case (1), we write out the page
        // For case (2), we divide the remaining entries equally between the
        // last two pages,
        // write them out, and update the parent's child pointers.
        for (int i = 0; i < entries.size(); i++) {
            int childPageCategory = (i == 0 ? BTreePageId.LEAF : BTreePageId.INTERNAL);
            int size = entries.get(i).size();
            if (size <= nentries) {
                // write out a page of entries
                byte[] internalPageBytes = convertToInternalPage(entries.get(i), npagebytes, keyType,
                        childPageCategory);
                BTreePageId internalPid = new BTreePageId(tableid, bf.numPages() + 1, BTreePageId.INTERNAL);
                bf.writePage(new BTreeInternalPage(internalPid, internalPageBytes, keyField));
            } else {
                // split the remaining entries in half
                ArrayList<BTreeEntry> secondToLastPg = new ArrayList<BTreeEntry>();
                ArrayList<BTreeEntry> lastPg = new ArrayList<BTreeEntry>();
                secondToLastPg.addAll(entries.get(i).subList(0, size / 2));
                lastPg.addAll(entries.get(i).subList(size / 2 + 1, size));

                // write out the last two pages of entries
                byte[] secondToLastPageBytes = convertToInternalPage(secondToLastPg, npagebytes, keyType,
                        childPageCategory);
                BTreePageId secondToLastPid = new BTreePageId(tableid, bf.numPages() + 1, BTreePageId.INTERNAL);
                bf.writePage(new BTreeInternalPage(secondToLastPid, secondToLastPageBytes, keyField));

                byte[] lastPageBytes = convertToInternalPage(lastPg, npagebytes, keyType, childPageCategory);
                BTreePageId lastPid = new BTreePageId(tableid, bf.numPages() + 1, BTreePageId.INTERNAL);
                bf.writePage(new BTreeInternalPage(lastPid, lastPageBytes, keyField));

                // update the parent by "pushing up" the next key
                BTreeEntry pushUpEntry = new BTreeEntry(entries.get(i).get(size / 2).getKey(), secondToLastPid,
                        lastPid);
                updateEntries(entries, bf, pushUpEntry, i + 1, nentries, npagebytes, keyType, tableid, keyField);
            }

        }
    }

    /**
     * 叶子节点的父是entry.每层(level)会有多个entry
     *
     * Recursive function to update the entries by adding a new Entry at a
     * particular level
     * 
     * @param levelEntryList - the list of entries,整个文件里所有页包含的entry，第n个list表示第n层的所有entry
     * @param bf - the BTreeFile
     * @param newEntry - the new entry
     * @param level - the level of the new entry (0 is closest to the leaf
     *        pages)
     * @param maxEntryNumPerPage - number of entries per page
     * @param pageSize - number of bytes per page
     * @param keyType - the type of the key field
     * @param tableId - the table id of this BTreeFile
     * @param keyField - the index of the key field
     * @throws IOException
     */
    private static void updateEntries(ArrayList<ArrayList<BTreeEntry>> levelEntryList, BTreeFile bf, BTreeEntry newEntry,
            int level, int maxEntryNumPerPage, int pageSize, Type keyType, int tableId, int keyField) throws IOException {
        while (levelEntryList.size() <= level) {
            levelEntryList.add(new ArrayList<BTreeEntry>());
        }

        int childPageCategory = (level == 0 ? BTreePageId.LEAF : BTreePageId.INTERNAL);
        //每个level上可能有多个page(1个page又包含多个entry)
        int size = levelEntryList.get(level).size();
        if (size > 0) {
            //将newEntry跟在此前最后一个entry后面,  pointer-prevEntry-pointer-newEntry
            BTreeEntry prev = levelEntryList.get(level).get(size - 1);
            levelEntryList.get(level).set(size - 1,
                    new BTreeEntry(prev.getKey(), prev.getLeftChild(), newEntry.getLeftChild()));
            // 此level上的entry总数超过2页
            if (size == maxEntryNumPerPage * 2 + 1) {
                // 输出一页entry
                ArrayList<BTreeEntry> pageEntries = new ArrayList<BTreeEntry>();
                pageEntries.addAll(levelEntryList.get(level).subList(0, maxEntryNumPerPage));
                byte[] internalPageBytes = convertToInternalPage(pageEntries, pageSize, keyType, childPageCategory);
                BTreePageId internalPid = new BTreePageId(tableId, bf.numPages() + 1, BTreePageId.INTERNAL);
                bf.writePage(new BTreeInternalPage(internalPid, internalPageBytes, keyField));

                // update the parent by "pushing up" the next key
                // 获取下一页的第一个entry(序号为maxEntryNum)的key作为新entry更新上一层
                BTreeEntry pushUpEntry = new BTreeEntry(levelEntryList.get(level).get(maxEntryNumPerPage).getKey(), internalPid, null);
                updateEntries(levelEntryList, bf, pushUpEntry, level + 1, maxEntryNumPerPage, pageSize, keyType, tableId, keyField);
                ArrayList<BTreeEntry> remainingEntries = new ArrayList<BTreeEntry>();
                remainingEntries.addAll(levelEntryList.get(level).subList(maxEntryNumPerPage + 1, size));
                levelEntryList.get(level).clear();
                levelEntryList.get(level).addAll(remainingEntries);
            }
        }
        levelEntryList.get(level).add(newEntry);
    }

    /**
     * 先写头,再写tuple,最后用0补齐
     *
     * Convert a set of tuples to a byte array in the format of a BTreeLeafPage
     * 
     * @param tupleAry - the set of tuple
     * @param pageBytes - number of bytes per page
     * @param numFields - number of fields in each tuple
     * @param typeAry - array containing the types of the tuples
     * @param keyField - the field of the tuples the B+ tree will be keyed on
     *        索引字段
     * @return a byte array which can be passed to the BTreeLeafPage constructor
     * @throws IOException
     */
    public static byte[] convertToLeafPage(ArrayList<Tuple> tupleAry, int pageBytes, int numFields, Type[] typeAry,
            int keyField) throws IOException {
        int recordSize = 0;
        for (int i = 0; i < numFields; i++) {
            recordSize += typeAry[i].getLen();
        }
        // 指针所占空间，左右父3个指针
        int pointerSize = 3 * BTreeLeafPage.INDEX_SIZE;
        // 可容纳记录数. 每条记录会有1bit的header表示slot是否被占用
        int recordNum = (pageBytes * 8 - pointerSize * 8) / (recordSize * 8 + 1);

        // 这些记录对应的header的大小
        int headerBytes = (recordNum / 8);
        if (headerBytes * 8 < recordNum) {
            headerBytes++; // ceiling
        }
        int headerBits = headerBytes * 8;

        // 输出
        ByteArrayOutputStream baos = new ByteArrayOutputStream(pageBytes);
        DataOutputStream dos = new DataOutputStream(baos);

        int actualRecordNum = tupleAry.size();
        if (actualRecordNum > recordNum) {
            actualRecordNum = recordNum;
        }

        // 先输出3个引用
        dos.writeInt(0); // parent pointer
        dos.writeInt(0); // left sibling pointer
        dos.writeInt(0); // right sibling pointer

        int i = 0;
        byte headerByte = 0;
        for (i = 0; i < headerBits; i++) {

            if (i < actualRecordNum) {
                // 每一条记录都将headerByte对应bit位置置为1
                headerByte |= (1 << (i % 8));
            }

            if (((i + 1) % 8) == 0) {
                // 够8位后输出
                dos.writeByte(headerByte);
                headerByte = 0;
            }
        }

        // 最后将余下的不够8bit的也输出
        if (i % 8 > 0) {
            dos.writeByte(headerByte);
        }

        // 对tuple进行排序后输出
        Collections.sort(tupleAry, new TupleComparator(keyField));
        for (int t = 0; t < actualRecordNum; t++) {
            TupleDesc td = tupleAry.get(t).getTupleDesc();
            for (int j = 0; j < td.numFields(); j++) {
                tupleAry.get(t).getField(j).serialize(dos);
            }
        }

        // 该页空余的部分补0
        int usedBytes = actualRecordNum * recordSize + headerBytes + pointerSize;
        int restPageBytes = pageBytes - usedBytes;
        for (i = 0; i < restPageBytes; i++) {
            dos.writeByte(0);
        }

        return baos.toByteArray();
    }

    /**
     * Comparator to sort BTreeEntry objects by key
     */
    public static class EntryComparator implements Comparator<BTreeEntry> {
        /**
         * Compare two entries based on their key field
         * 
         * @return -1 if e1 < e2, 1 if e1 > e2, 0 if e1 == e2
         */
        public int compare(BTreeEntry e1, BTreeEntry e2) {
            int cmp = 0;
            if (e1.getKey().compare(Op.LESS_THAN, e2.getKey())) {
                cmp = -1;
            } else if (e1.getKey().compare(Op.GREATER_THAN, e2.getKey())) {
                cmp = 1;
            }
            return cmp;
        }
    }

    /**
     * Comparator to sort BTreeEntry objects by key in descending order
     */
    public static class ReverseEntryComparator implements Comparator<BTreeEntry> {
        /**
         * Compare two entries based on their key field
         * 
         * @return -1 if e1 > e2, 1 if e1 < e2, 0 if e1 == e2
         */
        public int compare(BTreeEntry e1, BTreeEntry e2) {
            int cmp = 0;
            if (e1.getKey().compare(Op.GREATER_THAN, e2.getKey())) {
                cmp = -1;
            } else if (e1.getKey().compare(Op.LESS_THAN, e2.getKey())) {
                cmp = 1;
            }
            return cmp;
        }
    }

    /**
     * Convert a set of entries to a byte array in the format of a
     * BTreeInternalPage
     * 
     * @param entries - the set of entries
     * @param pageBytes - number of bytes per page
     * @param keyType - the type of the key field
     * @param childPageCategory - the category of the child pages (either
     *        internal or leaf)
     * @return a byte array which can be passed to the BTreeInternalPage
     *         constructor
     * @throws IOException
     */
    public static byte[] convertToInternalPage(ArrayList<BTreeEntry> entries, int pageBytes, Type keyType,
            int childPageCategory) throws IOException {
        int entryBytes = keyType.getLen() + BTreeInternalPage.INDEX_SIZE;
        // pointerBytes: one extra child pointer, parent pointer, child page
        // category
        int pointerBytes = 2 * BTreeLeafPage.INDEX_SIZE + 1;
        int entryNum = (pageBytes * 8 - pointerBytes * 8 - 1) / (entryBytes * 8 + 1);

        // 计算header的大小
        int headerBytes = (entryNum + 1) / 8;
        if (headerBytes * 8 < entryNum + 1) {
            headerBytes++; // ceiling
        }
        int headerBits = headerBytes * 8;

        ByteArrayOutputStream baos = new ByteArrayOutputStream(pageBytes);
        DataOutputStream dos = new DataOutputStream(baos);

        // write out the pointers and the header of the page,
        // then sort the entries and write them out.
        //
        // in the header, write a 1 for bits that correspond to entries we've
        // written and 0 for empty slots.
        int actualEntryNum = entries.size();
        if (actualEntryNum > entryNum) {
            actualEntryNum = entryNum;
        }

        dos.writeInt(0); // parent pointer
        dos.writeByte((byte) childPageCategory);

        int i = 0;
        byte headerByte = 0;

        for (i = 0; i < headerBits; i++) {
            if (i < actualEntryNum + 1)
                headerByte |= (1 << (i % 8));

            if (((i + 1) % 8) == 0) {
                dos.writeByte(headerByte);
                headerByte = 0;
            }
        }

        if (i % 8 > 0) {
            dos.writeByte(headerByte);
        }

        Collections.sort(entries, new EntryComparator());
        for (int e = 0; e < actualEntryNum; e++) {
            entries.get(e).getKey().serialize(dos);
        }

        for (int e = actualEntryNum; e < entryNum; e++) {
            for (int j = 0; j < keyType.getLen(); j++) {
                dos.writeByte(0);
            }
        }

        dos.writeInt(entries.get(0).getLeftChild().pageNumber());
        for (int e = 0; e < actualEntryNum; e++) {
            dos.writeInt(entries.get(e).getRightChild().pageNumber());
        }

        for (int e = actualEntryNum; e < entryNum; e++) {
            for (int j = 0; j < BTreeInternalPage.INDEX_SIZE; j++) {
                dos.writeByte(0);
            }
        }

        // pad the rest of the page with zeroes
        for (i = 0; i < (pageBytes - (entryNum * entryBytes + headerBytes + pointerBytes)); i++) {
            dos.writeByte(0);
        }

        return baos.toByteArray();

    }

    /**
     * Create a byte array in the format of a BTreeRootPtrPage
     * 
     * @param root - the page number of the root page
     * @param rootCategory - the category of the root page (leaf or internal)
     * @param header - the page number of the first header page
     * @return a byte array which can be passed to the BTreeRootPtrPage
     *         constructor
     * @throws IOException
     */
    public static byte[] convertToRootPtrPage(int root, int rootCategory, int header) throws IOException {

        ByteArrayOutputStream baos = new ByteArrayOutputStream(BTreeRootPtrPage.getPageSize());
        DataOutputStream dos = new DataOutputStream(baos);

        dos.writeInt(root); // root pointer
        dos.writeByte((byte) rootCategory); // root page category

        dos.writeInt(header); // header pointer

        return baos.toByteArray();
    }

}
