package org.apache.parquet.hadoop;

import me.yongshang.cbfm.CBFM;
import org.junit.Test;
import org.junit.Assert.*;


import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by Yongshang Wu on 2016/10/18.
 */
public class CBFMTest {
    public static void main(String[] args) {
        /*
        MessageType SCHEMA = MessageTypeParser.parseMessageType("" +
                "message m {" +
                "    required binary b;" +
                "    required int64 d;" +
                "  }" +
                "}");
        String[] PATH1 = {"b"};
        ColumnDescriptor C1 = SCHEMA.getColumnDescription(PATH1);
        String[] PATH2 = {"d"};
        ColumnDescriptor C2 = SCHEMA.getColumnDescription(PATH2);
        System.out.println(Arrays.toString(C1.getPath()));
        System.out.println(Arrays.toString(C2.getPath()));
        */
        CBFM.predicted_element_count_ = 1000;
        CBFM.desired_false_positive_probability_ = 0.1;
        CBFM.setIndexedDimensions(new String[]{"a", "b", "c"});
        CBFM.reducedimensions = new int[]{0,1};
//		CBFM.sizeLimit = 20;
        CBFM cbfm = new CBFM();
        cbfm.insert(cbfm.calculateIdxsForInsert(new byte[][]{"Test".getBytes(),"String".getBytes(),"Convert".getBytes()}));
        CBFM convertedCBFM = new CBFM(cbfm.compressTable());

    }
    @Test
    public void testStringConvert(){
        CBFM.DEBUG = true;
        CBFM.predicted_element_count_ = 100;
        CBFM.desired_false_positive_probability_ = 0.1;
        CBFM.setIndexedDimensions(new String[]{"a", "b", "c"});
        CBFM cbfm = new CBFM();
        cbfm.insert(cbfm.calculateIdxsForInsert(new byte[][]{"Test".getBytes(), "The".getBytes(), "Shit".getBytes()}));
        CBFM convertedCBFM = new CBFM(cbfm.compressTable());
        assertArrayEquals(cbfm.getTable(), convertedCBFM.getTable());
        assertTrue(convertedCBFM.contains(convertedCBFM.calculateIdxsForSearch(new byte[][]{"Test".getBytes(), "The".getBytes(), "Shit".getBytes()})));
    }

}
