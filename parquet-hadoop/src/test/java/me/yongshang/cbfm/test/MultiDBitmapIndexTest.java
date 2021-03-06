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

import me.yongshang.cbfm.MultiDBitmapIndex;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.*;

/**
 * Created by yongshangwu on 2016/11/8.
 */
public class MultiDBitmapIndexTest {
//    @Test
    public void testFunctionality3D(){
        // --Table partsupp:
        // ----A: int
        // ----B: string
        // ----C: double
        MultiDBitmapIndex index = new MultiDBitmapIndex(0.1, 10, 3);
        byte[][] bytes = new byte[][]{
                ByteBuffer.allocate(4).putInt(17).array(),
                "Test of Multi-Dimension".getBytes(),
                ByteBuffer.allocate(8).putDouble(77.7).array()
        };
        index.insert(bytes);
        assertTrue(index.contains(bytes));
    }

    @Test
    public void testMassively3D() throws IOException {
        int elementCount = 100000;
        byte[][][] bytes = new byte[elementCount][3][];
        // --Table partsupp:
        // ----A: int
        // ----B: string
        // ----C: double
        MultiDBitmapIndex index = new MultiDBitmapIndex(0.1, elementCount, 3);
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
            index.insert(bytes[i]);
            insertTime += (System.currentTimeMillis()-start);
        }
        index.displayUsage();
        long start = System.currentTimeMillis();
        for (byte[][] element : bytes) {
            assertTrue(index.contains(element));
        }
        // test non-member
        assertFalse(index.contains(new byte[][]{
                ByteBuffer.allocate(4).putInt(-17).array(),
                "FUCK".getBytes(),
                ByteBuffer.allocate(8).putDouble(-77.7).array()
        }));
        System.out.println("[MDBitmapIdx]\tavg insert time: "+insertTime/(double)elementCount+" ms, avg query time: "+(System.currentTimeMillis()-start)/(double)elementCount+" ms");
    }

    @Test
    public void testMassively1D() throws IOException {
        int elementCount = 100000;
        byte[][][] bytes = new byte[elementCount][1][];
        // --Table partsupp:
        // ----A: int
        MultiDBitmapIndex index = new MultiDBitmapIndex(0.1, elementCount, 1);
        FileReader reader = new FileReader("/Users/yongshangwu/Downloads/tpch_2_17_0/dbgen/partsupp.tbl");
        BufferedReader br = new BufferedReader(reader);
        long insertTime = 0;
        for(int i = 0; i < elementCount; i ++){
            String line = br.readLine();
            String[] tokens = line.split("\\|");
            bytes[i][0] = ByteBuffer.allocate(4).putInt(Integer.valueOf(tokens[2])).array();
            long start = System.currentTimeMillis();
            index.insert(bytes[i]);
            insertTime += (System.currentTimeMillis()-start);
        }
        index.displayUsage();
        long start = System.currentTimeMillis();
        for (byte[][] element : bytes) {
            assertTrue(index.contains(element));
        }
//        System.out.println("[MDBitmapIdx]\tavg insert time: "+insertTime/(double)elementCount+" ms, avg query time: "+(System.currentTimeMillis()-start)/(double)elementCount+" ms");
    }

    @Test
    public void testMassively2D() throws IOException {
        int elementCount = 100000;
        byte[][][] bytes = new byte[elementCount][2][];
        // --Table partsupp:
        // ----A: int
        // ----B: string
        MultiDBitmapIndex index = new MultiDBitmapIndex(0.1, elementCount, 2);
        FileReader reader = new FileReader("/Users/yongshangwu/Downloads/tpch_2_17_0/dbgen/partsupp.tbl");
        BufferedReader br = new BufferedReader(reader);
        long insertTime = 0;
        for(int i = 0; i < elementCount; i ++){
            String line = br.readLine();
            String[] tokens = line.split("\\|");
            bytes[i][0] = ByteBuffer.allocate(4).putInt(Integer.valueOf(tokens[2])).array();
            bytes[i][1] = tokens[4].getBytes();
            long start = System.currentTimeMillis();
            index.insert(bytes[i]);
            insertTime += (System.currentTimeMillis()-start);
        }
        index.displayUsage();
        long start = System.currentTimeMillis();
        for (byte[][] element : bytes) {
            assertTrue(index.contains(element));
        }
//        System.out.println("[MDBitmapIdx]\tavg insert time: "+insertTime/(double)elementCount+" ms, avg query time: "+(System.currentTimeMillis()-start)/(double)elementCount+" ms");
    }

//    @Test
    public void testMassively4D() throws IOException {
        int elementCount = 50000;
        byte[][][] bytes = new byte[elementCount][4][];
        // --Table partsupp:
        // ----A: int
        // ----B: string
        // ----C: double
        // ----D: int
        MultiDBitmapIndex index = new MultiDBitmapIndex(0.1, elementCount, 4);
        FileReader reader = new FileReader("/Users/yongshangwu/Downloads/tpch_2_17_0/dbgen/partsupp.tbl");
        BufferedReader br = new BufferedReader(reader);
        long insertTime = 0;
        for(int i = 0; i < elementCount; i ++){
            String line = br.readLine();
            String[] tokens = line.split("\\|");
            bytes[i][0] = ByteBuffer.allocate(4).putInt(Integer.valueOf(tokens[2])).array();
            bytes[i][1] = tokens[4].getBytes();
            bytes[i][2] = ByteBuffer.allocate(8).putDouble(Double.valueOf(tokens[3])).array();
            bytes[i][3] = ByteBuffer.allocate(4).putInt(Integer.valueOf(tokens[1])).array();
            long start = System.currentTimeMillis();
            index.insert(bytes[i]);
            insertTime += (System.currentTimeMillis()-start);
        }
        index.displayUsage();
        long start = System.currentTimeMillis();
        for (byte[][] element : bytes) {
            assertTrue(index.contains(element));
        }
        System.out.println("[MDBitmapIdx]\tavg insert time: "+insertTime/(double)elementCount+" ms, avg query time: "+(System.currentTimeMillis()-start)/(double)elementCount+" ms");
    }
}
