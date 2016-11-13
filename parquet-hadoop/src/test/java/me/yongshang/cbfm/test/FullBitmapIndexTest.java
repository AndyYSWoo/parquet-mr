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
import me.yongshang.cbfm.MultiDBitmapIndex;
import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by yongshangwu on 2016/11/9.
 */
public class FullBitmapIndexTest {
    @Test
    public void testCombination(){
        String[] dimensions = new String[]{"A", "B", "C", "D"};
        String[][] reducedDimensions = new String[][]{
                new String[]{"B", "C", "D"}
        };
        FullBitmapIndex index = new FullBitmapIndex(0.1, 1000, dimensions, reducedDimensions);
    }
    @Test
    public void testInsert(){
        String[] dimensions = new String[]{"A", "B", "C", "D"};
        String[][] reducedDimensions = new String[][]{
                new String[]{"B", "C", "D"}
        };
        FullBitmapIndex index = new FullBitmapIndex(0.1, 1000, dimensions, reducedDimensions);
        byte[][] bytes = new byte[][]{
                ByteBuffer.allocate(4).putInt(17).array(),
                "Test Full".getBytes(),
                ByteBuffer.allocate(8).putDouble(77.7).array(),
                ByteBuffer.allocate(4).putInt(7).array()
        };
        System.out.println("bytes: ");
        for(int i = 0; i < dimensions.length; ++i){
            System.out.println("\t"+dimensions[i]+": "+Arrays.toString(bytes[i]));
        }
        System.out.println();

        index.insert(bytes);
    }
    @Test
    public void testMassively() throws IOException {
        int elementCount = 100000;
        // --Table partsupp:
        // ----A: int
        // ----B: string
        // ----C: double
        // ----D: int
        String[] dimensions = new String[]{"A", "B", "C"};
        String[][] reducedDimensions = new String[][]{
                new String[]{"B", "C"},
        };
        byte[][][] bytes = new byte[elementCount][dimensions.length][];
        int indexCount = 1;
        ArrayList<FullBitmapIndex> indexes = new ArrayList<>();
        for (int i = 0; i < indexCount; i++) {
            indexes.add(new FullBitmapIndex(0.1, elementCount, dimensions, reducedDimensions));
        }
        FileReader reader = new FileReader("/Users/yongshangwu/Downloads/tpch_2_17_0/dbgen/partsupp.tbl");
        BufferedReader br = new BufferedReader(reader);
        long insertTime = 0;
        for(int i = 0; i < elementCount; i ++){
            String line = br.readLine();
            String[] tokens = line.split("\\|");
            bytes[i][0] = ByteBuffer.allocate(4).putInt(Integer.valueOf(tokens[2])).array();
            bytes[i][1] = tokens[4].getBytes();
            bytes[i][2] = ByteBuffer.allocate(8).putDouble(Double.valueOf(tokens[3])).array();
//            bytes[i][3] = ByteBuffer.allocate(4).putInt(Integer.valueOf(tokens[1])).array();
            long start = System.currentTimeMillis();
            for (FullBitmapIndex index : indexes) {
                index.insert(bytes[i]);
            }
            insertTime += (System.currentTimeMillis()-start);
        }
        for (FullBitmapIndex index : indexes) {
            index.displayUsage();
            for (int i = 0; i < elementCount; i++) {
                assertTrue(index.contains(new String[]{"A", "B"},
                        new byte[][]{bytes[i][0], bytes[i][1]}));
                assertFalse(index.contains(new String[]{"A", "B"}, new byte[][]{{1},{1}}));

            }
            System.out.println();
        }
    }
    @Test
    public void testCompare() throws IOException {
        int elementCount = 100000;
        // --Table partsupp:
        // ----A: int
        // ----B: string
        // ----C: double
        // ----D: int
        String[] dimensions = new String[]{"A", "B", "C"};
        String[][] reducedDimensions = new String[][]{
                new String[]{"B", "C"}
        };
        FullBitmapIndex fullIndex = new FullBitmapIndex(0.1, elementCount, dimensions, reducedDimensions);
        MultiDBitmapIndex multiDIndex = new MultiDBitmapIndex(0.1, elementCount, dimensions.length);
        FileReader reader = new FileReader("/Users/yongshangwu/Downloads/tpch_2_17_0/dbgen/partsupp.tbl");
        BufferedReader br = new BufferedReader(reader);
        long insertTime = 0;
        for(int i = 0; i < elementCount; i ++){
            String line = br.readLine();
            String[] tokens = line.split("\\|");
            byte[][] bytes = new byte[dimensions.length][];
            bytes[0] = ByteBuffer.allocate(4).putInt(Integer.valueOf(tokens[2])).array();
            bytes[1] = tokens[4].getBytes();
            bytes[2] = ByteBuffer.allocate(8).putDouble(Double.valueOf(tokens[3])).array();
//            bytes[i][3] = ByteBuffer.allocate(4).putInt(Integer.valueOf(tokens[1])).array();
            long start = System.currentTimeMillis();
            fullIndex.insert(bytes);
            multiDIndex.insert(bytes);
            insertTime += (System.currentTimeMillis()-start);
        }
        fullIndex.displayUsage();
        System.out.println();
        multiDIndex.displayUsage();
        System.out.println();

        RoaringBitmap[] bitmaps = new RoaringBitmap[8];
    }

}
