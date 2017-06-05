package com.bow.camptotheca.db;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

public class CatalogTest {

    private static String name = "test";

    private String nameThisTestRun;

    @Before
    public void addTables() throws Exception {
        Database.reset();
        Database.getCatalog().clear();
        nameThisTestRun = SystemTestUtil.getUUID();
        Database.getCatalog().addTable(new TestUtil.SkeletonFile(-1, Utility.getTupleDesc(2)), nameThisTestRun);
        Database.getCatalog().addTable(new TestUtil.SkeletonFile(-2, Utility.getTupleDesc(2)), name);
    }

    /**
     * Unit test for Catalog.getTupleDesc()
     */
    @Test
    public void getTupleDesc() throws Exception {
        TupleDesc expected = Utility.getTupleDesc(2);
        TupleDesc actual = Database.getCatalog().getTupleDesc(-1);

        assertEquals(expected, actual);
    }

    /**
     * Unit test for Catalog.getTableId()
     */
    @Test
    public void getTableId() {
        assertEquals(-2, Database.getCatalog().getTableId(name));
        assertEquals(-1, Database.getCatalog().getTableId(nameThisTestRun));

    }

    /**
     * Unit test for Catalog.getDatabaseFile()
     */
    @Test
    public void getDatabaseFile() throws Exception {
        DbFile f = Database.getCatalog().getDatabaseFile(-1);
        assertEquals(-1, f.getId());
    }

}
