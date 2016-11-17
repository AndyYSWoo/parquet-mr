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
package me.yongshang.cbfm.test;

import me.yongshang.cbfm.FullBitmapIndex;
import me.yongshang.cbfm.MDBF;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by yongshangwu on 2016/11/17.
 */
public class MDBFTest {
    @Test
    public void testFunctionaliy(){
        int elementCount = 100;
        MDBF.ON = true;
        MDBF.desiredFalsePositiveProbability = 0.1;
        MDBF.dimensions = new String[]{"A", "B", "C"};
        MDBF index = new MDBF(elementCount);

        index.insert(MDBF.dimensions, new byte[][]{
                {1},
                {2},
                {3}
        });

        assertFalse(index.contains(MDBF.dimensions, new byte[][]{
                {7},
                {2},
                {3}
        }));
    }

    @Test
    public void testMassively() throws IOException {
        int elementCount = 800000;

        String filePath = "/Users/yongshangwu/Desktop/index-count:"+elementCount;
        DataOutput out = new DataOutputStream(new FileOutputStream(filePath));
        // --Table partsup
        // ----A: int
        // ----B: string
        // ----C: double
        // ----D: int
        String[] dimensions = new String[]{"A", "B", "C"};
        byte[][][] bytes = new byte[elementCount][dimensions.length][];

        MDBF.ON = true;
        MDBF.desiredFalsePositiveProbability = 0.1;
        MDBF.dimensions = dimensions;
        MDBF index = new MDBF(elementCount);

        FileReader reader = new FileReader("/Users/yongshangwu/Downloads/tpch_2_17_0/dbgen/partsupp.tbl");
        BufferedReader br = new BufferedReader(reader);
        long insertTime = 0;
        for(int i = 0; i < elementCount; i ++){
            String line = br.readLine();
            String[] tokens = line.split("\\|");
            bytes[i][0] = ByteBuffer.allocate(4).putInt(Integer.valueOf(tokens[2])).array();
            bytes[i][1] = tokens[4].getBytes();
            bytes[i][2] = ByteBuffer.allocate(8).putDouble(Double.valueOf(tokens[3])).array();
            long start = System.currentTimeMillis();
            index.insert(dimensions, bytes[i]);
            insertTime += (System.currentTimeMillis()-start);
        }
        bytes = null;
        System.gc();
//        for (int i = 0; i < elementCount; i++) {
//            assertTrue(index.contains(dimensions,
//                    new byte[][]{bytes[i][0], bytes[i][1], bytes[i][2]}));
//
//        }
    }

    @Test
    public void testSerialization() throws IOException {
        int elementCount = 200000;

        String filePath = "/Users/yongshangwu/Desktop/[MDBF]"+elementCount;
        DataOutput out = new DataOutputStream(new FileOutputStream(filePath));
        // --Table partsup
        // ----A: int
        // ----B: string
        // ----C: double
        // ----D: int
        String[] dimensions = new String[]{"A", "B", "C"};
        byte[][][] bytes = new byte[elementCount][dimensions.length][];

        MDBF.ON = true;
        MDBF.desiredFalsePositiveProbability = 0.1;
        MDBF.dimensions = dimensions;
        MDBF index = new MDBF(elementCount);

        FileReader reader = new FileReader("/Users/yongshangwu/Downloads/tpch_2_17_0/dbgen/partsupp.tbl");
        BufferedReader br = new BufferedReader(reader);
        long insertTime = 0;
        for(int i = 0; i < elementCount; i ++){
            String line = br.readLine();
            String[] tokens = line.split("\\|");
            bytes[i][0] = ByteBuffer.allocate(4).putInt(Integer.valueOf(tokens[2])).array();
            bytes[i][1] = tokens[4].getBytes();
            bytes[i][2] = ByteBuffer.allocate(8).putDouble(Double.valueOf(tokens[3])).array();
            long start = System.currentTimeMillis();
            index.insert(dimensions, bytes[i]);
            insertTime += (System.currentTimeMillis()-start);
        }
        index.serialize(out);
        index = null;
        System.gc();

        DataInput in = new DataInputStream(new FileInputStream(filePath));
        MDBF generatedIndex = new MDBF(in);
        for (int i = 0; i < elementCount; i++) {
            assertTrue(generatedIndex.contains(dimensions,
                    new byte[][]{bytes[i][0], bytes[i][1], bytes[i][2]}));

        }
    }
}
