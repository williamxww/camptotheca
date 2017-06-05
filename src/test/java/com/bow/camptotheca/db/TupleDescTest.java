package com.bow.camptotheca.db;

import java.util.NoSuchElementException;

import com.bow.camptotheca.db.TupleDesc;
import com.bow.camptotheca.db.Type;
import com.bow.camptotheca.db.Utility;
import org.junit.Test;

import static org.junit.Assert.*;

public class TupleDescTest {

    /**
     * Unit test for TupleDesc.combine()
     */
    @Test
    public void combine() {
        TupleDesc td1, td2, td3;

        td1 = Utility.getTupleDesc(1, "td1");
        td2 = Utility.getTupleDesc(2, "td2");

        td3 = TupleDesc.merge(td1, td2);
        assertEquals(3, td3.numFields());
        assertEquals(3 * Type.INT_TYPE.getLen(), td3.getSize());

        for (int i = 0; i < 3; ++i) {
            assertEquals(Type.INT_TYPE, td3.getFieldType(i));
        }
        assertEquals(combinedStringArrays(td1, td2, td3), true);

        td3 = TupleDesc.merge(td2, td1);
        assertEquals(3, td3.numFields());
        assertEquals(3 * Type.INT_TYPE.getLen(), td3.getSize());
        for (int i = 0; i < 3; ++i)
            assertEquals(Type.INT_TYPE, td3.getFieldType(i));
        assertEquals(combinedStringArrays(td2, td1, td3), true);

        td3 = TupleDesc.merge(td2, td2);
        assertEquals(4, td3.numFields());
        assertEquals(4 * Type.INT_TYPE.getLen(), td3.getSize());
        for (int i = 0; i < 4; ++i)
            assertEquals(Type.INT_TYPE, td3.getFieldType(i));
        assertEquals(combinedStringArrays(td2, td2, td3), true);
    }

    /**
     * Ensures that combined's field names = td1's field names + td2's field
     * names
     */
    private boolean combinedStringArrays(TupleDesc td1, TupleDesc td2, TupleDesc combined) {
        for (int i = 0; i < td1.numFields(); i++) {
            if (!(((td1.getFieldName(i) == null) && (combined.getFieldName(i) == null))
                    || td1.getFieldName(i).equals(combined.getFieldName(i)))) {
                return false;
            }
        }

        for (int i = td1.numFields(); i < td1.numFields() + td2.numFields(); i++) {
            if (!(((td2.getFieldName(i - td1.numFields()) == null) && (combined.getFieldName(i) == null))
                    || td2.getFieldName(i - td1.numFields()).equals(combined.getFieldName(i)))) {
                return false;
            }
        }

        return true;
    }

    /**
     * Unit test for TupleDesc.getType()
     */
    @Test
    public void getType() {
        int[] lengths = new int[] { 1, 10 };

        for (int len : lengths) {
            TupleDesc td = Utility.getTupleDesc(len);
            for (int i = 0; i < len; ++i) {
                assertEquals(Type.INT_TYPE, td.getFieldType(i));
            }
        }
    }

    /**
     * 根据字段名找该字段的序号(在第几列)
     */
    @Test
    public void fieldNameToIndex() {
        int[] lengths = new int[] { 1, 2 };
        String prefix = "test";

        for (int len : lengths) {
            // Make sure you retrieve well-named fields
            TupleDesc td = Utility.getTupleDesc(len, prefix);
            for (int i = 0; i < len; ++i) {
                // 根据字段名找出该字段是此行的第几列
                assertEquals(i, td.fieldNameToIndex(prefix + i));
            }

            // Make sure you throw exception for non-existent fields
            try {
                // 根据不存在的字段名找字段的序号
                td.fieldNameToIndex("foo");

                // 字段名为Null
                td.fieldNameToIndex(null);

                // 此tuple没有字段名
                td = Utility.getTupleDesc(len);
                td.fieldNameToIndex(prefix);

            } catch (NoSuchElementException e) {
                // expected to get here
            }

        }
    }

    /**
     * TupleDesc的总长度就是每个字段长度之和，每个字段大小是有字段类型决定
     */
    @Test
    public void getSize() {
        int[] lengths = new int[] { 1, 2, 1000 };

        for (int len : lengths) {
            TupleDesc td = Utility.getTupleDesc(len);
            assertEquals(len * Type.INT_TYPE.getLen(), td.getSize());
        }
    }

    /**
     * TupleDesc对应着多少字段
     */
    @Test
    public void numFields() {
        int[] lengths = new int[] { 1, 2 };

        for (int len : lengths) {
            TupleDesc td = Utility.getTupleDesc(len);
            assertEquals(len, td.numFields());
        }
    }

    @Test
    public void testEquals() {
        TupleDesc singleInt = new TupleDesc(new Type[] { Type.INT_TYPE });
        TupleDesc singleInt2 = new TupleDesc(new Type[] { Type.INT_TYPE });
        TupleDesc intString = new TupleDesc(new Type[] { Type.INT_TYPE, Type.STRING_TYPE });

        assertFalse(singleInt.equals(null));
        assertFalse(singleInt.equals(new Object()));

        assertTrue(singleInt.equals(singleInt));
        assertTrue(singleInt.equals(singleInt2));
        assertTrue(singleInt2.equals(singleInt));
        assertTrue(intString.equals(intString));

        assertFalse(singleInt.equals(intString));
        assertFalse(singleInt2.equals(intString));
        assertFalse(intString.equals(singleInt));
        assertFalse(intString.equals(singleInt2));
    }


}
