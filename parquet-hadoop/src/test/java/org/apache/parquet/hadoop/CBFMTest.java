/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.hadoop;

import me.yongshang.cbfm.CBFM;
import org.apache.commons.lang.ArrayUtils;
import org.apache.parquet.io.api.Binary;
import org.junit.Test;
import org.junit.Assert.*;


import java.nio.ByteBuffer;
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
    @Test
    public void testBinary(){
        assertArrayEquals(Binary.fromString("Test").getBytes(), "Test".getBytes());
    }

    @Test
    public void testBytes(){
        System.out.println("name1: "+Arrays.toString("Jack".getBytes()));
        System.out.println("\t"+Arrays.toString(Binary.fromString("Jack").getBytes()));
        System.out.println("name2: "+Arrays.toString("Jason".getBytes()));
        System.out.println("name3: "+Arrays.toString("James".getBytes()));
        System.out.println("name4: "+Arrays.toString("Someone".getBytes()));
        System.out.println("name5: "+Arrays.toString("Customer#000000003".getBytes()));
        System.out.println("name6: "+Arrays.toString("MG9kdTD2WBHm".getBytes()));
        System.out.println("name7: "+Arrays.toString("11-719-748-3364".getBytes()));
        System.out.println("name8: "+Arrays.toString(" deposits eat slyly ironic, even instructions. express foxes detect slyly. blithely even accounts abov".getBytes()));

        System.out.println("age1: "+Arrays.toString(ByteBuffer.allocate(4).putInt(21).array()));
        byte[] age1 = ByteBuffer.allocate(4).putInt(21).array();
        ArrayUtils.reverse(age1);
        System.out.println("\t"+Arrays.toString(age1));
        System.out.println("age2: "+Arrays.toString(ByteBuffer.allocate(4).putInt(35).array()));
        System.out.println("age3: "+Arrays.toString(ByteBuffer.allocate(4).putInt(40).array()));
        System.out.println("age4: "+Arrays.toString(ByteBuffer.allocate(4).putInt(35).array()));

        System.out.println("balance1: "+Arrays.toString(ByteBuffer.allocate(8).putDouble(1000).array()));
        byte[] balance1 = ByteBuffer.allocate(8).putDouble(1000).array();
        ArrayUtils.reverse(balance1);
        System.out.println("\t"+Arrays.toString(balance1));
        System.out.println("balance2: "+Arrays.toString(ByteBuffer.allocate(8).putDouble(5000).array()));
        System.out.println("balance3: "+Arrays.toString(ByteBuffer.allocate(8).putDouble(2000).array()));
        System.out.println("balance4: "+Arrays.toString(ByteBuffer.allocate(8).putDouble(3000).array()));


    }
}
