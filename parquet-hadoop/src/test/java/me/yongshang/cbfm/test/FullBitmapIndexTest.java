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
import me.yongshang.cbfm.MultiDBitmapIndex;
import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
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
        int elementCount = 10000;

        String filePath = "/Users/yongshangwu/Desktop/[compressedCBFM]"+elementCount;
        DataOutput out = new DataOutputStream(new FileOutputStream(filePath));
        // --Table partsup
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
            bytes[i] = null;
            insertTime += (System.currentTimeMillis()-start);
        }
        bytes = null;
        System.gc();
        for (FullBitmapIndex index : indexes) {
            index.displayUsage();
            index.serialize(out);
//            for (int i = 0; i < elementCount; i++) {
//                assertTrue(index.contains(new String[]{"A", "B"},
//                        new byte[][]{bytes[i][0], bytes[i][1]}));
//                assertFalse(index.contains(new String[]{"A", "B"}, new byte[][]{{1},{1}}));
//
//            }
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
    @Test
    public void testSerializeRoaringBitmap() throws IOException {
        RoaringBitmap bitmap = new RoaringBitmap();
        bitmap.add(1);
        ByteBuffer byteBuffer = ByteBuffer.allocate(bitmap.serializedSizeInBytes());
        bitmap.serialize(new DataOutputStream(new OutputStream() {
            ByteBuffer buffer;
            public OutputStream init(ByteBuffer buffer){
                this.buffer = buffer;
                return this;
            }
            public void flush(){};
            public void close(){};
            @Override
            public void write(int b) throws IOException {
                buffer.put((byte)b);
            }
            public void write(byte[] b) {
                buffer.put(b);
            }
            public void write(byte[] b, int off, int l) {
                buffer.put(b,off,l);
            }
        }.init(byteBuffer)));
        RoaringBitmap newBitmap = new RoaringBitmap();
        newBitmap.deserialize(new DataInputStream(new ByteArrayInputStream(byteBuffer.array())));
        assertTrue(newBitmap.contains(1));
    }
    @Test
    public void testNewConstructor(){
        FullBitmapIndex.ON = true;
        FullBitmapIndex.falsePositiveProbability = 0.1;
        FullBitmapIndex.setDimensions(new String[]{"A", "B", "C"}, new String[][]{new String[]{"B", "C"}});
        FullBitmapIndex index = new FullBitmapIndex(10);
        index.insert(new byte[][]{
                {1},
                {2},
                {3}
        });
        assertTrue(index.contains(new String[]{"A", "B"}, new byte[][]{
                {1},
                {2}
        }));
    }
    @Test
    public void testCompression(){
        FullBitmapIndex.ON = true;
        FullBitmapIndex.falsePositiveProbability = 0.1;
        FullBitmapIndex.setDimensions(new String[]{"A", "B", "C"}, new String[][]{new String[]{"B", "C"}});
        FullBitmapIndex index = new FullBitmapIndex(10);
        index.insert(new byte[][]{
                {1},
                {2},
                {3}
        });
        String str = index.compress();
        System.out.println(str);

        FullBitmapIndex generatedIndex = new FullBitmapIndex(str);
        assertTrue(generatedIndex.contains(new String[]{"A", "B"}, new byte[][]{
                {1},
                {2}
        }));
        assertFalse(generatedIndex.contains(new String[]{"A", "B"}, new byte[][]{
                {1},
                {1}
        }));
        assertFalse(generatedIndex.contains(new String[]{"A", "B"}, new byte[][]{
                {2},
                {2}
        }));
    }

    @Test
    public void testSerialization() throws IOException{
        FullBitmapIndex.ON = true;
        FullBitmapIndex.falsePositiveProbability = 0.1;
        FullBitmapIndex.setDimensions(new String[]{"A", "B", "C"}, new String[][]{new String[]{"B", "C"}});
        FullBitmapIndex index = new FullBitmapIndex(10);
        index.insert(new byte[][]{
                {1},
                {2},
                {3}
        });

        String filePath = "/Users/yongshangwu/Desktop/serialization";
        DataOutput out = new DataOutputStream(new FileOutputStream(filePath));
        index.serialize(out);
        System.out.println(index.compress());

        DataInput in = new DataInputStream(new FileInputStream(filePath));
        FullBitmapIndex generatedIndex = new FullBitmapIndex(in);
        System.out.println(generatedIndex.compress());
        assertTrue(generatedIndex.contains(new String[]{"A", "B"}, new byte[][]{
                {1},
                {2}
        }));
    }
    @Test
    public void testContainsNull(){
        FullBitmapIndex.ON = true;
        FullBitmapIndex.falsePositiveProbability = 0.1;
        FullBitmapIndex.setDimensions(new String[]{"A", "B", "C"}, new String[][]{new String[]{"B", "C"}});
        FullBitmapIndex index = new FullBitmapIndex(10);
        index.insert(new byte[][]{
                {1},
                {2},
                {3}
        });
        assertFalse(index.contains(new String[]{"A"}, new byte[][]{
                {3},
                null,
                null
        }));
    }
    @Test
    public void testNoReduced() throws IOException {
        FullBitmapIndex.ON = true;
        FullBitmapIndex.falsePositiveProbability = 0.1;
        FullBitmapIndex.setDimensions(new String[]{"A"}, new String[][]{});
        FullBitmapIndex index = new FullBitmapIndex(200000);

        FileReader reader = new FileReader("/Users/yongshangwu/Downloads/tpch_2_17_0/dbgen/partsupp.tbl");
        BufferedReader br = new BufferedReader(reader);
        for(int i = 0; i < 800000; ++i){
            String line = br.readLine();
//            if(0<=i && i<=200000){
//            if(200000<=i && i<=400000){
//            if(400000<=i && i<=600000){
            if(200000<=i && i<=400000){
                String[] tokens = line.split("\\|");
                index.insert(new byte[][]{
                        ByteBuffer.allocate(4).putInt(Integer.valueOf(tokens[0])).array(),
//                        ByteBuffer.allocate(4).putInt(Integer.valueOf(tokens[1])).array(),
//                        ByteBuffer.allocate(8).putDouble(Double.valueOf(tokens[3])).array()
                });
            }
        }

        assertFalse(index.contains(new String[]{"A"}, new byte[][]{
                ByteBuffer.allocate(4).putInt(1).array()
        }));
    }

    @Test
    public void fixTest() throws IOException{
        int elementCount = 277051;
        FullBitmapIndex.ON = true;
        FullBitmapIndex.falsePositiveProbability = 0.1;
        FullBitmapIndex.setDimensions(new String[]{"A", "B", "C"}, new String[][]{{"A", "C"}});
        FullBitmapIndex index = new FullBitmapIndex(elementCount);

        FileReader reader = new FileReader("/Users/yongshangwu/Downloads/tpch_2_17_0/dbgen/part.tbl");
        BufferedReader br = new BufferedReader(reader);
        for(int i = 0; i < elementCount; ++i){
            String line = br.readLine();
            String[] tokens = line.split("\\|");
            index.insert(new byte[][]{
                    ByteBuffer.allocate(4).putInt(Integer.valueOf(tokens[5])).array(),
                    tokens[3].getBytes(),
                    tokens[6].getBytes()
            });
        }

        String serializePath = "/Users/yongshangwu/Desktop/temp";
        File serializeFile = new File(serializePath);
        DataOutput out = new DataOutputStream(new FileOutputStream(serializeFile));
        index.serialize(out);
        DataInput in = new DataInputStream(new FileInputStream(serializeFile));
        FullBitmapIndex serialIndex = new FullBitmapIndex(in);
        assertTrue(serialIndex.contains(new String[]{"B", "C"}, new byte[][]{
                "Brand#55".getBytes(),
                "SM BAG".getBytes()
        }));
        assertFalse(serialIndex.contains(new String[]{"B", "C"}, new byte[][]{
                "GIVEAFUCK".getBytes(),
                "SUCKADICK".getBytes()
        }));
        assertFalse(serialIndex.contains(new String[]{"B"}, new byte[][]{
                "GIVEAFUCK".getBytes(),
        }));
        serializeFile.delete();
    }
}
