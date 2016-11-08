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

import me.yongshang.cbfm.CBFM;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.column.statistics.DoubleStatistics;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.RowGroupFilter;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;

import static org.apache.parquet.filter2.predicate.FilterApi.*;
import static org.junit.Assert.assertEquals;


/**
 * Created by yongshangwu on 2016/10/24.
 */
public class CBFMIntegrateTest {
    @Test
    public void testWriteRead() throws Exception {
        CBFM.ON = true;
        CBFM.desired_false_positive_probability_ = 0.1;
        CBFM.setIndexedDimensions(new String[]{"a","b","c"});
        CBFM.reducedimensions = new int[]{3};
        // File
        TemporaryFolder temp = new TemporaryFolder();
        File testFile = temp.newFile();
        testFile.delete();
        Path path = new Path(testFile.toURI());
        Configuration configuration = new Configuration();
        CompressionCodecName codec = CompressionCodecName.UNCOMPRESSED;
        // Schema
        MessageType schema = MessageTypeParser.parseMessageType(
                "message tbl { " +
                        "required binary a (UTF8); " +
                        "required int32 b; " +
                        "required double c;" +
                        "}");
        ColumnDescriptor a = schema.getColumnDescription(new String[]{"a"});
        ColumnDescriptor b = schema.getColumnDescription(new String[]{"b"});
        ColumnDescriptor c = schema.getColumnDescription(new String[]{"c"});

        Operators.BinaryColumn aColumn = binaryColumn("a");
        Operators.IntColumn bColumn = intColumn("b");
        Operators.DoubleColumn cColumn = doubleColumn("c");
        // Write
        ParquetFileWriter w = new ParquetFileWriter(configuration, schema, path);
        w.start();
        w.startBlock(1);
        w.startColumn(a, 1, codec);
        BytesInput bytes = BytesInput.from("Test".getBytes());
        w.writeDataPage(1, (int)bytes.size(), bytes, new BinaryStatistics(), Encoding.BIT_PACKED, Encoding.BIT_PACKED, Encoding.PLAIN);
        w.endColumn();
        w.startColumn(b, 1, codec);
        w.writeDataPage(1, 4, BytesInput.from(ByteBuffer.allocate(4).putInt(7).array()), new IntStatistics(), Encoding.BIT_PACKED, Encoding.BIT_PACKED, Encoding.PLAIN);
        w.endColumn();
        w.startColumn(c, 1, codec);
        w.writeDataPage(1, 8, BytesInput.from(ByteBuffer.allocate(8).putDouble(17.7).array()),new DoubleStatistics(), Encoding.BIT_PACKED, Encoding.BIT_PACKED, Encoding.PLAIN);
        w.endColumn();
        w.endBlock();
        w.end(new HashMap<String, String>());
        // Read
        ParquetMetadata readFooter = ParquetFileReader.readFooter(configuration, path);
        List<BlockMetaData> blocks = readFooter.getBlocks();
        List<BlockMetaData> filtered = RowGroupFilter.filterRowGroupsByCBFM(FilterCompat.get(and(eq(aColumn, Binary.fromString("Test")), eq(bColumn, 7))), blocks, schema);
        assertEquals(blocks.size(), filtered.size());
    }
}
