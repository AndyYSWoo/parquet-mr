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
package org.apache.parquet.filter2.compat;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import me.yongshang.cbfm.CBFM;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.io.api.Binary;
import org.junit.Test;

import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.filter2.predicate.Operators.IntColumn;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import static org.apache.parquet.filter2.predicate.FilterApi.*;
import static org.junit.Assert.assertEquals;
import static org.apache.parquet.hadoop.TestInputFormat.makeBlockFromStats;
import static org.junit.Assert.assertTrue;

public class TestRowGroupFilter {
  @Test
  public void testApplyRowGroupFilters() {

    List<BlockMetaData> blocks = new ArrayList<BlockMetaData>();

    IntStatistics stats1 = new IntStatistics();
    stats1.setMinMax(10, 100);
    stats1.setNumNulls(4);
    BlockMetaData b1 = makeBlockFromStats(stats1, 301);
    blocks.add(b1);

    IntStatistics stats2 = new IntStatistics();
    stats2.setMinMax(8, 102);
    stats2.setNumNulls(0);
    BlockMetaData b2 = makeBlockFromStats(stats2, 302);
    blocks.add(b2);

    IntStatistics stats3 = new IntStatistics();
    stats3.setMinMax(100, 102);
    stats3.setNumNulls(12);
    BlockMetaData b3 = makeBlockFromStats(stats3, 303);
    blocks.add(b3);


    IntStatistics stats4 = new IntStatistics();
    stats4.setMinMax(0, 0);
    stats4.setNumNulls(304);
    BlockMetaData b4 = makeBlockFromStats(stats4, 304);
    blocks.add(b4);


    IntStatistics stats5 = new IntStatistics();
    stats5.setMinMax(50, 50);
    stats5.setNumNulls(7);
    BlockMetaData b5 = makeBlockFromStats(stats5, 305);
    blocks.add(b5);

    IntStatistics stats6 = new IntStatistics();
    stats6.setMinMax(0, 0);
    stats6.setNumNulls(12);
    BlockMetaData b6 = makeBlockFromStats(stats6, 306);
    blocks.add(b6);

    MessageType schema = MessageTypeParser.parseMessageType("message Document { optional int32 foo; }");
    IntColumn foo = intColumn("foo");

    List<BlockMetaData> filtered = RowGroupFilter.filterRowGroups(FilterCompat.get(eq(foo, 50)), blocks, schema);
    assertEquals(Arrays.asList(b1, b2, b5), filtered);

    filtered = RowGroupFilter.filterRowGroups(FilterCompat.get(notEq(foo, 50)), blocks, schema);
    assertEquals(Arrays.asList(b1, b2, b3, b4, b5, b6), filtered);

    filtered = RowGroupFilter.filterRowGroups(FilterCompat.get(eq(foo, null)), blocks, schema);
    assertEquals(Arrays.asList(b1, b3, b4, b5, b6), filtered);

    filtered = RowGroupFilter.filterRowGroups(FilterCompat.get(notEq(foo, null)), blocks, schema);
    assertEquals(Arrays.asList(b1, b2, b3, b5, b6), filtered);

    filtered = RowGroupFilter.filterRowGroups(FilterCompat.get(eq(foo, 0)), blocks, schema);
    assertEquals(Arrays.asList(b6), filtered);
  }

  @Test
  public void testCBFMRowGroupFilterForString(){
    MessageType schema = MessageTypeParser.parseMessageType("message tbl { required binary a (UTF8); }");
    Operators.BinaryColumn a = binaryColumn("a");

    CBFM.predicted_element_count_ = 10;
    CBFM.desired_false_positive_probability_ = 0.1;
    CBFM.setIndexedDimensions(new String[]{"a"});

    List<BlockMetaData> blocks = new ArrayList<>();

    BlockMetaData b1 = new BlockMetaData();
    CBFM cbfm1 = new CBFM();
    cbfm1.insert(cbfm1.calculateIdxsForInsert(new byte[][]{"Test".getBytes()}));
    b1.setIndexTableStr(cbfm1.compressTable());
    blocks.add(b1);
    List<BlockMetaData> filtered = RowGroupFilter.filterRowGroupsByCBFM(
            FilterCompat.get(eq(a, Binary.fromString("Test"))),
            blocks, schema);
    assertEquals(1, filtered.size());
  }

  @Test
  public void testCBFMRGFForInt(){
    MessageType schema = MessageTypeParser.parseMessageType("message tbl { required int32 b; }");
    IntColumn b = intColumn("b");

    CBFM.predicted_element_count_ = 10;
    CBFM.desired_false_positive_probability_ = 0.1;
    CBFM.setIndexedDimensions(new String[]{"b"});

    List<BlockMetaData> blocks = new ArrayList<>();

    BlockMetaData b1 = new BlockMetaData();
    CBFM cbfm1 = new CBFM();
    cbfm1.insert(cbfm1.calculateIdxsForInsert(new byte[][]{ByteBuffer.allocate(4).putInt(17).array()}));
    b1.setIndexTableStr(cbfm1.compressTable());
    blocks.add(b1);
    List<BlockMetaData> filtered = RowGroupFilter.filterRowGroupsByCBFM(
            FilterCompat.get(eq(b, 17)),
            blocks, schema);
    assertEquals(1, filtered.size());
  }

  @Test
  public void testCBFMRGFForLong(){
    MessageType schema = MessageTypeParser.parseMessageType("message tbl { required int64 c; }");
    Operators.LongColumn c = longColumn("c");

    CBFM.predicted_element_count_ = 10;
    CBFM.desired_false_positive_probability_ = 0.1;
    CBFM.setIndexedDimensions(new String[]{"c"});

    List<BlockMetaData> blocks = new ArrayList<>();

    BlockMetaData b1 = new BlockMetaData();
    CBFM cbfm1 = new CBFM();
    cbfm1.insert(cbfm1.calculateIdxsForInsert(new byte[][]{ByteBuffer.allocate(8).putLong(17).array()}));
    b1.setIndexTableStr(cbfm1.compressTable());
    blocks.add(b1);
    List<BlockMetaData> filtered = RowGroupFilter.filterRowGroupsByCBFM(
            FilterCompat.get(eq(c, (long)17)),
            blocks, schema);
    assertEquals(1, filtered.size());
  }

  @Test
  public void testCBFMRGFForDouble(){
    MessageType schema = MessageTypeParser.parseMessageType("message tbl { required double d; }");
    Operators.DoubleColumn d = doubleColumn("d");

    CBFM.predicted_element_count_ = 10;
    CBFM.desired_false_positive_probability_ = 0.1;
    CBFM.setIndexedDimensions(new String[]{"d"});

    List<BlockMetaData> blocks = new ArrayList<>();

    BlockMetaData b1 = new BlockMetaData();
    CBFM cbfm1 = new CBFM();
    cbfm1.insert(cbfm1.calculateIdxsForInsert(new byte[][]{ByteBuffer.allocate(8).putDouble(17.7).array()}));
    b1.setIndexTableStr(cbfm1.compressTable());
    blocks.add(b1);
    List<BlockMetaData> filtered = RowGroupFilter.filterRowGroupsByCBFM(
            FilterCompat.get(eq(d, 17.7)),
            blocks, schema);
    assertEquals(1, filtered.size());
  }

  @Test
  public void testCBFMRGFForFloat(){
    MessageType schema = MessageTypeParser.parseMessageType("message tbl { required double e; }");
    Operators.FloatColumn e = floatColumn("e");

    CBFM.predicted_element_count_ = 10;
    CBFM.desired_false_positive_probability_ = 0.1;
    CBFM.setIndexedDimensions(new String[]{"e"});

    List<BlockMetaData> blocks = new ArrayList<>();

    BlockMetaData b1 = new BlockMetaData();
    CBFM cbfm1 = new CBFM();
    cbfm1.insert(cbfm1.calculateIdxsForInsert(new byte[][]{ByteBuffer.allocate(4).putFloat(17.7f).array()}));
    b1.setIndexTableStr(cbfm1.compressTable());
    blocks.add(b1);
    List<BlockMetaData> filtered = RowGroupFilter.filterRowGroupsByCBFM(
            FilterCompat.get(eq(e, 17.7f)),
            blocks, schema);
    assertEquals(1, filtered.size());
  }

  @Test
  public void testCBFMRGFFull(){
    MessageType schema = MessageTypeParser.parseMessageType(
              "message tbl { " +
                      "required binary a (UTF8); " +
                      "required int32 b; " +
                      "required double c;" +
                      "}");
    Operators.BinaryColumn a = binaryColumn("a");
    IntColumn b = intColumn("b");
    Operators.DoubleColumn c = doubleColumn("c");

    CBFM.predicted_element_count_ = 100;
    CBFM.desired_false_positive_probability_ = 0.1;
    CBFM.setIndexedDimensions(new String[]{"a","b","c"});

    List<BlockMetaData> blocks = new ArrayList<>();

    BlockMetaData b1 = new BlockMetaData();
    CBFM cbfm1 = new CBFM();
    cbfm1.insert(cbfm1.calculateIdxsForInsert(new byte[][]{
            "Test".getBytes(),
            ByteBuffer.allocate(4).putInt(7).array(),
            ByteBuffer.allocate(8).putDouble(17.7).array()
    }));

    b1.setIndexTableStr(cbfm1.compressTable());
    blocks.add(b1);
    List<BlockMetaData> filtered = RowGroupFilter.filterRowGroupsByCBFM(
            FilterCompat.get(
                    and(
                            eq(a,Binary.fromString("Test")),
                            and(eq(b,7), eq(c, 17.7)))),
            blocks, schema);
    assertEquals(1, filtered.size());
  }

  @Test
  public void testCBFMRGFReduced(){
    MessageType schema = MessageTypeParser.parseMessageType(
            "message tbl { " +
                    "required binary a (UTF8); " +
                    "required int32 b; " +
                    "required double c;" +
                    "}");
    Operators.BinaryColumn a = binaryColumn("a");
    IntColumn b = intColumn("b");
    Operators.DoubleColumn c = doubleColumn("c");

    CBFM.predicted_element_count_ = 100;
    CBFM.desired_false_positive_probability_ = 0.1;
    CBFM.setIndexedDimensions(new String[]{"a","b","c"});
    // reduce the combination of b & c
    CBFM.reducedimensions = new int[]{0x3};

    List<BlockMetaData> blocks = new ArrayList<>();

    BlockMetaData b1 = new BlockMetaData();
    CBFM cbfm1 = new CBFM();
    cbfm1.insert(cbfm1.calculateIdxsForInsert(new byte[][]{
            "Test".getBytes(),
            ByteBuffer.allocate(4).putInt(7).array(),
            ByteBuffer.allocate(8).putDouble(17.7).array()
    }));

    b1.setIndexTableStr(cbfm1.compressTable());
    blocks.add(b1);
    List<BlockMetaData> filtered = RowGroupFilter.filterRowGroupsByCBFM(
            FilterCompat.get(
                    and(eq(a,Binary.fromString("Test")),eq(b,7))),
            blocks, schema);
    assertEquals(1, filtered.size());
    filtered = RowGroupFilter.filterRowGroupsByCBFM(
            FilterCompat.get(
                    and(eq(b,7), eq(c,17.7))),
            blocks, schema);
    assertEquals(1, filtered.size());
  }
}
