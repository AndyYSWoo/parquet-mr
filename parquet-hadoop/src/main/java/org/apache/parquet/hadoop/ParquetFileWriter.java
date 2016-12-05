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

import static org.apache.parquet.Log.DEBUG;
import static org.apache.parquet.format.Util.writeFileMetaData;
import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE;
import static org.apache.parquet.hadoop.ParquetWriter.MAX_PADDING_SIZE_DEFAULT;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.*;
import java.util.Map.Entry;

import me.yongshang.cbfm.CBFM;
import me.yongshang.cbfm.CMDBF;
import me.yongshang.cbfm.FullBitmapIndex;
import me.yongshang.cbfm.MDBF;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.parquet.Log;
import org.apache.parquet.Preconditions;
import org.apache.parquet.Strings;
import org.apache.parquet.Version;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.EncodingStats;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.column.values.dictionary.DictionaryValuesReader;
import org.apache.parquet.filter2.compat.RowGroupFilter;
import org.apache.parquet.hadoop.ParquetOutputFormat.JobSummaryLevel;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.GlobalMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopStreams;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.TypeUtil;
import org.xerial.snappy.Snappy;
import org.xerial.snappy.SnappyFramedInputStream;

/**
 * Internal implementation of the Parquet file writer as a block container
 *
 * @author Julien Le Dem
 *
 */
public class ParquetFileWriter {
  // CBFM support
  private byte[][][] rows;
  private int rowIndex;

  private static final Log LOG = Log.getLog(ParquetFileWriter.class);

  private static ParquetMetadataConverter metadataConverter = new ParquetMetadataConverter();

  public static final String PARQUET_METADATA_FILE = "_metadata";
  public static final String MAGIC_STR = "PAR1";
  public static final byte[] MAGIC = MAGIC_STR.getBytes(Charset.forName("ASCII"));
  public static final String PARQUET_COMMON_METADATA_FILE = "_common_metadata";
  public static final int CURRENT_VERSION = 1;

  // need to supply a buffer size when setting block size. this is the default
  // for hadoop 1 to present. copying it avoids loading DFSConfigKeys.
  private static final int DFS_BUFFER_SIZE_DEFAULT = 4096;
  // visible for testing
  static final Set<String> BLOCK_FS_SCHEMES = new HashSet<String>();
  static {
    BLOCK_FS_SCHEMES.add("hdfs");
    BLOCK_FS_SCHEMES.add("webhdfs");
    BLOCK_FS_SCHEMES.add("viewfs");
  }

  private static boolean supportsBlockSize(FileSystem fs) {
    return BLOCK_FS_SCHEMES.contains(fs.getUri().getScheme());
  }

  // File creation modes
  public static enum Mode {
    CREATE,
    OVERWRITE
  }

  private final MessageType schema;
  private final FSDataOutputStream out;
//  private final FSDataOutputStream
  private final AlignmentStrategy alignment;

  // file data
  private List<BlockMetaData> blocks = new ArrayList<BlockMetaData>();

  // row group data
  private BlockMetaData currentBlock; // appended to by endColumn

  // row group data set at the start of a row group
  private long currentRecordCount; // set in startBlock

  // column chunk data accumulated as pages are written
  private EncodingStats.Builder encodingStatsBuilder;
  private Set<Encoding> currentEncodings;
  private long uncompressedLength;
  private long compressedLength;
  private Statistics currentStatistics; // accumulated in writePage(s)

  // column chunk data set at the start of a column
  private CompressionCodecName currentChunkCodec; // set in startColumn
  private ColumnPath currentChunkPath;            // set in startColumn
  private PrimitiveTypeName currentChunkType;     // set in startColumn
  private long currentChunkValueCount;            // set in startColumn
  private long currentChunkFirstDataPage;         // set in startColumn (out.pos())
  private long currentChunkDictionaryPageOffset;  // set in writeDictionaryPage

  // CBFM write
  private Path file;
  private boolean overwriteFlag;
  private long dfsBlockSize;
  private Configuration configuration;
  private boolean firstConstructor;

  private ColumnDescriptor currentColumnDescriptor;
  /**
   * Captures the order in which methods should be called
   *
   * @author Julien Le Dem
   *
   */
  private enum STATE {
    NOT_STARTED {
      STATE start() {
        return STARTED;
      }
    },
    STARTED {
      STATE startBlock() {
        return BLOCK;
      }
      STATE end() {
        return ENDED;
      }
    },
    BLOCK  {
      STATE startColumn() {
        return COLUMN;
      }
      STATE endBlock() {
        return STARTED;
      }
    },
    COLUMN {
      STATE endColumn() {
        return BLOCK;
      };
      STATE write() {
        return this;
      }
    },
    ENDED;

    STATE start() throws IOException { return error(); }
    STATE startBlock() throws IOException { return error(); }
    STATE startColumn() throws IOException { return error(); }
    STATE write() throws IOException { return error(); }
    STATE endColumn() throws IOException { return error(); }
    STATE endBlock() throws IOException { return error(); }
    STATE end() throws IOException { return error(); }

    private final STATE error() throws IOException {
      throw new IOException("The file being written is in an invalid state. Probably caused by an error thrown previously. Current state: " + this.name());
    }
  }

  private STATE state = STATE.NOT_STARTED;

  /**
   * @param configuration Hadoop configuration
   * @param schema the schema of the data
   * @param file the file to write to
   * @throws IOException if the file can not be created
   */
  public ParquetFileWriter(Configuration configuration, MessageType schema,
                           Path file) throws IOException {
    this(configuration, schema, file, Mode.CREATE, DEFAULT_BLOCK_SIZE,
            MAX_PADDING_SIZE_DEFAULT);
  }

  /**
   * @param configuration Hadoop configuration
   * @param schema the schema of the data
   * @param file the file to write to
   * @param mode file creation mode
   * @throws IOException if the file can not be created
   */
  public ParquetFileWriter(Configuration configuration, MessageType schema,
                           Path file, Mode mode) throws IOException {
    this(configuration, schema, file, mode, DEFAULT_BLOCK_SIZE,
            MAX_PADDING_SIZE_DEFAULT);
  }

  /**
   * @param configuration Hadoop configuration
   * @param schema the schema of the data
   * @param file the file to write to
   * @param mode file creation mode
   * @param rowGroupSize the row group size
   * @throws IOException if the file can not be created
   */
  public ParquetFileWriter(Configuration configuration, MessageType schema,
                           Path file, Mode mode, long rowGroupSize,
                           int maxPaddingSize)
          throws IOException {
    firstConstructor = true;
    this.configuration = configuration;
    this.file = file;
    TypeUtil.checkValidWriteSchema(schema);
    this.schema = schema;
    FileSystem fs = file.getFileSystem(configuration);
    boolean overwriteFlag = (mode == Mode.OVERWRITE);
    this.overwriteFlag = overwriteFlag;
    if (supportsBlockSize(fs)) {
      // use the default block size, unless row group size is larger
      long dfsBlockSize = Math.max(fs.getDefaultBlockSize(file), rowGroupSize);
      this.dfsBlockSize = dfsBlockSize;
      this.alignment = PaddingAlignment.get(
              dfsBlockSize, rowGroupSize, maxPaddingSize);
      this.out = fs.create(file, overwriteFlag, DFS_BUFFER_SIZE_DEFAULT,
              fs.getDefaultReplication(file), dfsBlockSize);
    } else {
      this.alignment = NoAlignment.get(rowGroupSize);
      this.out = fs.create(file, overwriteFlag);
    }

    this.encodingStatsBuilder = new EncodingStats.Builder();
  }

  /**
   * FOR TESTING ONLY.
   *
   * @param configuration Hadoop configuration
   * @param schema the schema of the data
   * @param file the file to write to
   * @param rowAndBlockSize the row group size
   * @throws IOException if the file can not be created
   */
  ParquetFileWriter(Configuration configuration, MessageType schema,
                    Path file, long rowAndBlockSize, int maxPaddingSize)
          throws IOException {
    firstConstructor = false;
    this.configuration = configuration;
    this.file = file;
    FileSystem fs = file.getFileSystem(configuration);
    this.schema = schema;
    this.alignment = PaddingAlignment.get(
            rowAndBlockSize, rowAndBlockSize, maxPaddingSize);
    this.dfsBlockSize = rowAndBlockSize;
    this.out = fs.create(file, true, DFS_BUFFER_SIZE_DEFAULT,
            fs.getDefaultReplication(file), rowAndBlockSize);
    this.encodingStatsBuilder = new EncodingStats.Builder();
  }

  /**
   * start the file
   * @throws IOException
   */
  public void start() throws IOException {
    state = state.start();
    if (DEBUG) LOG.debug(out.getPos() + ": start");
    out.write(MAGIC);
  }

  /**
   * start a block
   * @param recordCount the record count in this block
   * @throws IOException
   */
  public void startBlock(long recordCount) throws IOException {
    state = state.startBlock();
    if (DEBUG) LOG.debug(out.getPos() + ": start block");
//    out.write(MAGIC); // TODO: add a magic delimiter

    alignment.alignForRowGroup(out);

    currentBlock = new BlockMetaData();
    currentRecordCount = recordCount;
    if(CBFM.ON) {
      rows = new byte[(int) recordCount][CBFM.dimension][];
    }else if(FullBitmapIndex.ON){
      rows = new byte[(int) recordCount][FullBitmapIndex.dimensions.length][];
    }else if(MDBF.ON){
      rows = new byte[(int) recordCount][MDBF.dimensions.length][];
    }else if(CMDBF.ON){
      rows = new byte[(int) recordCount][CMDBF.dimensions.length][];
    }
  }

  /**
   * start a column inside a block
   * @param descriptor the column descriptor
   * @param valueCount the value count in this column
   * @param compressionCodecName
   * @throws IOException
   */
  public void startColumn(ColumnDescriptor descriptor,
                          long valueCount,
                          CompressionCodecName compressionCodecName) throws IOException {
    state = state.startColumn();
    encodingStatsBuilder.clear();
    currentEncodings = new HashSet<Encoding>();
    currentColumnDescriptor = descriptor;
    currentChunkPath = ColumnPath.get(descriptor.getPath());
    currentChunkType = descriptor.getType();
    currentChunkCodec = compressionCodecName;
    currentChunkValueCount = valueCount;
    currentChunkFirstDataPage = out.getPos();
    compressedLength = 0;
    uncompressedLength = 0;
    // need to know what type of stats to initialize to
    // better way to do this?
    currentStatistics = Statistics.getStatsBasedOnType(currentChunkType);
    if(CBFM.ON || FullBitmapIndex.ON || MDBF.ON || CMDBF.ON){
      rowIndex = 0;
    }
  }

  /**
   * writes a dictionary page page
   * @param dictionaryPage the dictionary page
   */
  public void writeDictionaryPage(DictionaryPage dictionaryPage) throws IOException {
    System.out.println("===========DictionaryPage: "+Arrays.toString(dictionaryPage.getBytes().toByteArray()));
    state = state.write();
    if (DEBUG) LOG.debug(out.getPos() + ": write dictionary page: " + dictionaryPage.getDictionarySize() + " values");
    currentChunkDictionaryPageOffset = out.getPos();
    int uncompressedSize = dictionaryPage.getUncompressedSize();
    int compressedPageSize = (int)dictionaryPage.getBytes().size(); // TODO: fix casts
    metadataConverter.writeDictionaryPageHeader(
            uncompressedSize,
            compressedPageSize,
            dictionaryPage.getDictionarySize(),
            dictionaryPage.getEncoding(),
            out);
    long headerSize = out.getPos() - currentChunkDictionaryPageOffset;
    this.uncompressedLength += uncompressedSize + headerSize;
    this.compressedLength += compressedPageSize + headerSize;
    if (DEBUG) LOG.debug(out.getPos() + ": write dictionary page content " + compressedPageSize);
    dictionaryPage.getBytes().writeAllTo(out);
    encodingStatsBuilder.addDictEncoding(dictionaryPage.getEncoding());
    currentEncodings.add(dictionaryPage.getEncoding());
  }


  /**
   * writes a single page
   * @param valueCount count of values
   * @param uncompressedPageSize the size of the data once uncompressed
   * @param bytes the compressed data for the page without header
   * @param rlEncoding encoding of the repetition level
   * @param dlEncoding encoding of the definition level
   * @param valuesEncoding encoding of values
   */
  @Deprecated
  public void writeDataPage(
          int valueCount, int uncompressedPageSize,
          BytesInput bytes,
          Encoding rlEncoding,
          Encoding dlEncoding,
          Encoding valuesEncoding) throws IOException {
    state = state.write();
    long beforeHeader = out.getPos();
    if (DEBUG) LOG.debug(beforeHeader + ": write data page: " + valueCount + " values");
    int compressedPageSize = (int)bytes.size();
    metadataConverter.writeDataPageHeader(
            uncompressedPageSize, compressedPageSize,
            valueCount,
            rlEncoding,
            dlEncoding,
            valuesEncoding,
            out);
    long headerSize = out.getPos() - beforeHeader;
    this.uncompressedLength += uncompressedPageSize + headerSize;
    this.compressedLength += compressedPageSize + headerSize;
    if (DEBUG) LOG.debug(out.getPos() + ": write data page content " + compressedPageSize);
    bytes.writeAllTo(out);
    encodingStatsBuilder.addDataEncoding(valuesEncoding);
    currentEncodings.add(rlEncoding);
    currentEncodings.add(dlEncoding);
    currentEncodings.add(valuesEncoding);
  }

  /**
   * writes a single page
   * @param valueCount count of values
   * @param uncompressedPageSize the size of the data once uncompressed
   * @param bytes the compressed data for the page without header
   * @param rlEncoding encoding of the repetition level
   * @param dlEncoding encoding of the definition level
   * @param valuesEncoding encoding of values
   */
  public void writeDataPage(
          int valueCount, int uncompressedPageSize,
          BytesInput bytes,
          Statistics statistics,
          Encoding rlEncoding,
          Encoding dlEncoding,
          Encoding valuesEncoding) throws IOException {
    state = state.write();
    if(CBFM.ON){
      throw new RuntimeException("USING writeDataPage RATHER THAN writeDataPages, ADAPT TO THIS METHOD");
    }

    long beforeHeader = out.getPos();
    if (DEBUG) LOG.debug(beforeHeader + ": write data page: " + valueCount + " values");
    int compressedPageSize = (int)bytes.size();
    metadataConverter.writeDataPageHeader(
            uncompressedPageSize, compressedPageSize,
            valueCount,
            statistics,
            rlEncoding,
            dlEncoding,
            valuesEncoding,
            out);
    long headerSize = out.getPos() - beforeHeader;
    this.uncompressedLength += uncompressedPageSize + headerSize;
    this.compressedLength += compressedPageSize + headerSize;
    if (DEBUG) LOG.debug(out.getPos() + ": write data page content " + compressedPageSize);
    bytes.writeAllTo(out);
    currentStatistics.mergeStatistics(statistics);
    encodingStatsBuilder.addDataEncoding(valuesEncoding);
    currentEncodings.add(rlEncoding);
    currentEncodings.add(dlEncoding);
    currentEncodings.add(valuesEncoding);
  }

  /**
   * writes a number of pages at once
   * @param bytes bytes to be written including page headers
   * @param uncompressedTotalPageSize total uncompressed size (without page headers)
   * @param compressedTotalPageSize total compressed size (without page headers)
   * @throws IOException
   */
  void writeDataPages(BytesInput bytes,
                      long uncompressedTotalPageSize,
                      long compressedTotalPageSize,
                      Statistics totalStats,
                      Set<Encoding> rlEncodings,
                      Set<Encoding> dlEncodings,
                      List<Encoding> dataEncodings) throws IOException {
    state = state.write();
    if (DEBUG) LOG.debug(out.getPos() + ": write data pages");
    long headersSize = bytes.size() - compressedTotalPageSize;
    this.uncompressedLength += uncompressedTotalPageSize + headersSize;
    this.compressedLength += compressedTotalPageSize + headersSize;
    if (DEBUG) LOG.debug(out.getPos() + ": write data pages content");

    String[] indexedDimensions = null;
    if(CBFM.ON){
      indexedDimensions = CBFM.indexedColumns;
    }else if(FullBitmapIndex.ON){
      indexedDimensions = FullBitmapIndex.dimensions;
    }else if(MDBF.ON){
      indexedDimensions = MDBF.dimensions;
    }

    if(CBFM.ON || FullBitmapIndex.ON || MDBF.ON){
      // Decompress content (or not)
      byte[] originalBytes = bytes.toByteArray();
//      byte[] uncompressedByteArray = Arrays.copyOfRange(originalBytes, (int) headersSize, originalBytes.length);
      byte[] uncompressedByteArray = decompress(bytes, headersSize, compressedTotalPageSize, uncompressedTotalPageSize);
      // Find corresponding column
      String currentColumnName = currentChunkPath.toArray()[currentChunkPath.size()-1];
      for(int i = 0; i < indexedDimensions.length; ++i){
        if(indexedDimensions[i].equals(currentColumnName)){
          System.out.println("=====Column: "+currentColumnName);
          System.out.println("==========byte size: "+bytes.size()+", headersize: "+headersSize
                  +", uncompressedTotalPageSize: "+uncompressedTotalPageSize+", compressedTotalPageSize: "+compressedTotalPageSize);
          System.out.println("==========uncompressedByteArrayBound: "+uncompressedByteArray.length);
          System.out.println("==========Original: "+Arrays.toString(Arrays.copyOfRange(originalBytes, 0, Math.min(originalBytes.length, 1000))));
          System.out.println("==========Uncompressed: "+Arrays.toString(Arrays.copyOfRange(uncompressedByteArray, 0, Math.min(uncompressedByteArray.length, 1000))));
          int stringOffset = 0;
          for(int j = 0; j < currentRecordCount; ++j){        // for every row
            switch (currentChunkType){
              case INT32:
                rows[j][i] = Arrays.copyOfRange(uncompressedByteArray, j*4, (j+1)*4);
                break;
              case DOUBLE:
                rows[j][i] = Arrays.copyOfRange(uncompressedByteArray, j*8, j*8+8);
                break;
              case BINARY:
//                System.out.println("==========StringOff: "+stringOffset);
                if(stringOffset == 0){// ignore first str: 2000 3 15/7
                  int placeHolderLen = BytesUtils.readIntLittleEndian(uncompressedByteArray, 0);
                  stringOffset = 4 + placeHolderLen;
                }
                int strLen = BytesUtils.readIntLittleEndian(uncompressedByteArray, stringOffset);
                stringOffset += 4;
                rows[j][i] = Arrays.copyOfRange(uncompressedByteArray, stringOffset, stringOffset + strLen);
//                System.out.println(Arrays.toString(rows[j][i]));
                stringOffset += strLen;
                break;
            }
          }
          break;
        }
      }
    }
    bytes.writeAllTo(out);
    encodingStatsBuilder.addDataEncodings(dataEncodings);
    if (rlEncodings.isEmpty()) {
      encodingStatsBuilder.withV2Pages();
    }
    currentEncodings.addAll(rlEncodings);
    currentEncodings.addAll(dlEncodings);
    currentEncodings.addAll(dataEncodings);
    currentStatistics = totalStats;
  }

  /**
   * Same as the other writeDataPages,
   * only added header info to strip to only data.
   * @param dataRange
   * @throws IOException
   */
  void writeDataPages(BytesInput bytes,
                      long uncompressedTotalPageSize,
                      long compressedTotalPageSize,
                      Statistics totalStats,
                      Set<Encoding> rlEncodings,
                      Set<Encoding> dlEncodings,
                      List<Encoding> dataEncodings,
                      HashMap<Integer, Integer> dataRange) throws IOException {
    state = state.write();
    if (DEBUG) LOG.debug(out.getPos() + ": write data pages");
    long headersSize = bytes.size() - compressedTotalPageSize;
    this.uncompressedLength += uncompressedTotalPageSize + headersSize;
    this.compressedLength += compressedTotalPageSize + headersSize;
    if (DEBUG) LOG.debug(out.getPos() + ": write data pages content");

    String[] indexedDimensions = null;
    if(CBFM.ON){
      indexedDimensions = CBFM.indexedColumns;
    }else if(FullBitmapIndex.ON){
      indexedDimensions = FullBitmapIndex.dimensions;
    }else if(MDBF.ON){
      indexedDimensions = MDBF.dimensions;
    }else if(CMDBF.ON){
      indexedDimensions = CMDBF.dimensions;
    }

    if(CBFM.ON || FullBitmapIndex.ON || MDBF.ON || CMDBF.ON){
      // Sort DataRange
      ArrayList<Integer> keys = new ArrayList<>();
      keys.addAll(dataRange.keySet());
      Collections.sort(keys);
      // Find corresponding column
      String currentColumnName = currentChunkPath.toArray()[currentChunkPath.size()-1];
      for(int i = 0; i < indexedDimensions.length; ++i){
        if(indexedDimensions[i].equals(currentColumnName)){
          // Decompress content (or not)
          byte[] originalBytes = bytes.toByteArray();
//          byte[] uncompressedByteArray = stripHeaders(originalBytes, (int)uncompressedLength, dataRange);
          /*
          System.out.println("=====Column: "+currentColumnName);
          System.out.println("==========byte size: "+bytes.size()+", headersize: "+headersSize
                  +", uncompressedTotalPageSize: "+uncompressedTotalPageSize+", compressedTotalPageSize: "+compressedTotalPageSize);
          System.out.println("==========uncompressedByteArrayBound: "+uncompressedByteArray.length);
          System.out.println("==========Original: "+Arrays.toString(Arrays.copyOfRange(originalBytes, 0, Math.min(originalBytes.length, 100))));
          System.out.println("==========Uncompressed: "+Arrays.toString(Arrays.copyOfRange(uncompressedByteArray, 0, Math.min(uncompressedByteArray.length, 100))));
          */
          // Old way
          /*
          int stringOffset = 0;
          for(int j = 0; j < currentRecordCount; ++j){        // for every row
            switch (currentChunkType){
              case INT32:
                rows[j][i] = Arrays.copyOfRange(uncompressedByteArray, j*4, (j+1)*4);
                break;
              case DOUBLE:
                rows[j][i] = Arrays.copyOfRange(uncompressedByteArray, j*8, j*8+8);
                break;
              case BINARY:
//                if(stringOffset == 0){
//                  int placeHolderLen = BytesUtils.readIntLittleEndian(uncompressedByteArray, 0);
//                  stringOffset = 4 + placeHolderLen;
//                }
                int strLen = BytesUtils.readIntLittleEndian(uncompressedByteArray, stringOffset);
                stringOffset += 4;
                rows[j][i] = Arrays.copyOfRange(uncompressedByteArray, stringOffset, stringOffset + strLen);
                stringOffset += strLen;
                break;
            }
          }
          */
          // New way, without creating new space
          int offset = 0;
          for (int j = 0; j < currentRecordCount; j++) {
            offset = adjustOffset(offset, keys, dataRange);
            switch (currentChunkType){
              case INT32:
                rows[j][i] = Arrays.copyOfRange(originalBytes, offset, offset+4);
                offset += 4;
                break;
              case DOUBLE:
                rows[j][i] = Arrays.copyOfRange(originalBytes, offset, offset+8);
                offset += 8;
                break;
              case BINARY:
                int strLen = BytesUtils.readIntLittleEndian(originalBytes, offset);
                offset += 4;
                rows[j][i] = Arrays.copyOfRange(originalBytes, offset, offset + strLen);
                offset += strLen;
                break;
            }
          }
          break;
        }
      }
    }
    bytes.writeAllTo(out);
    encodingStatsBuilder.addDataEncodings(dataEncodings);
    if (rlEncodings.isEmpty()) {
      encodingStatsBuilder.withV2Pages();
    }
    currentEncodings.addAll(rlEncodings);
    currentEncodings.addAll(dlEncodings);
    currentEncodings.addAll(dataEncodings);
    currentStatistics = totalStats;
  }

  /**
   * Skip headers by adjusting offset rather than creating new arrays
   * @param offset
   * @param keys
   * @param dataRange
     * @return
     */
  private int adjustOffset(int offset, ArrayList<Integer> keys, HashMap<Integer, Integer> dataRange){
    if(keys.isEmpty()) return offset;
    int start = keys.get(0);
    int len = dataRange.get(start);
    if(offset == start){
      offset += len;
      keys.remove(0);
    }
    return offset;
  }

  private byte[] stripHeaders(byte[] bytes, int dataSize ,HashMap<Integer, Integer> dataRange){
    ByteBuffer data = ByteBuffer.allocate(dataSize);
    ArrayList<Integer> keys = new ArrayList<>();
    keys.addAll(dataRange.keySet());
    Collections.sort(keys);
    for (int i = 0; i < keys.size(); i++) {
      int offset = keys.get(i);
      int len = dataRange.get(offset);
      data.put(bytes, offset, len);
    }
    return data.array();
  }

  private byte[] decompress(BytesInput compressed,
                            long headersSize,
                            long compressedTotalPageSize,
                            long uncompressedTotalPageSize) throws IOException {
    CodecFactory.BytesDecompressor decompressor = new CodecFactory(new Configuration(), (int)compressedTotalPageSize)
//            .createDecompressor(CompressionCodecName.SNAPPY);
            .createDecompressor(CompressionCodecName.UNCOMPRESSED);
    BytesInput compressedBytes = BytesInput.from(compressed.toByteArray(), (int)headersSize, (int)compressedTotalPageSize);
    BytesInput uncompressedBytes = decompressor.decompress(compressedBytes, (int)uncompressedTotalPageSize);
    return uncompressedBytes.toByteArray();
  }

  /**
   * end a column (once all rep, def and data have been written)
   * @throws IOException
   */
  public void endColumn() throws IOException {
    state = state.endColumn();
    if (DEBUG) LOG.debug(out.getPos() + ": end column");
    currentBlock.addColumn(ColumnChunkMetaData.get(
            currentChunkPath,
            currentChunkType,
            currentChunkCodec,
            encodingStatsBuilder.build(),
            currentEncodings,
            currentStatistics,
            currentChunkFirstDataPage,
            currentChunkDictionaryPageOffset,
            currentChunkValueCount,
            compressedLength,
            uncompressedLength));
    this.currentBlock.setTotalByteSize(currentBlock.getTotalByteSize() + uncompressedLength);
    this.uncompressedLength = 0;
    this.compressedLength = 0;
  }

  /**
   * ends a block once all column chunks have been written
   * @throws IOException
   */
  public void endBlock() throws IOException {
    state = state.endBlock();
    if (DEBUG) LOG.debug(out.getPos() + ": end block");
    currentBlock.setRowCount(currentRecordCount);
    long start = System.currentTimeMillis();
    if(FullBitmapIndex.ON){
      currentBlock.index = new FullBitmapIndex(currentRecordCount);
      if(rows[0][0] != null){
        for (int i = 0; i < rows.length; i++) {
          currentBlock.index.insert(rows[i]);
          rows[i] = null;
        }
      }
    }

    if(MDBF.ON){
      currentBlock.mdbfIndex = new MDBF(currentRecordCount);
      if(rows[0][0] != null){
        for (int i = 0; i < rows.length; i++) {
          currentBlock.mdbfIndex.insert(rows[i]);
          rows[i] = null;
        }
      }
    }

    if(CMDBF.ON){
      currentBlock.cmdbfIndex = new CMDBF(currentRecordCount);
      if(rows[0][0] != null){
        for (int i = 0; i < rows.length; i++) {
          currentBlock.cmdbfIndex.insert(rows[i]);
          rows[i] = null;
        }
      }
    }
    if(RowGroupFilter.checkIndexed(schema.getColumns())){
      writeTime(System.currentTimeMillis() - start);
    }
    rows = null;
    blocks.add(currentBlock);
  }

  private void writeTime(long time){
    try {
      File resultFile = new File(RowGroupFilter.filePath+"index-create-time");
      if(!resultFile.exists()) resultFile.createNewFile();
      PrintWriter pw = new PrintWriter(new FileWriter(resultFile, true));
      pw.write(time+" ms.\n");
      pw.flush();
      pw.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private FSDataOutputStream getOut(Path filePath) throws IOException {
    FileSystem fs = file.getFileSystem(configuration);
    FileSystem newFS = filePath.getFileSystem(configuration);
    FSDataOutputStream out = null;
    if(firstConstructor){
      if(supportsBlockSize(fs)){
        out = fs.create(filePath, overwriteFlag, DFS_BUFFER_SIZE_DEFAULT,
                newFS.getDefaultReplication(filePath), dfsBlockSize);
      }else{
        out = fs.create(filePath, overwriteFlag);
      }
    }else{
      out = fs.create(filePath, true, DFS_BUFFER_SIZE_DEFAULT,
              fs.getDefaultReplication(filePath), dfsBlockSize);
    }
    return out;
  }

  public void appendFile(Configuration conf, Path file) throws IOException {
    ParquetFileReader.open(conf, file).appendTo(this);
  }

  public void appendRowGroups(FSDataInputStream file,
                              List<BlockMetaData> rowGroups,
                              boolean dropColumns) throws IOException {
    appendRowGroups(HadoopStreams.wrap(file), rowGroups, dropColumns);
  }

  public void appendRowGroups(SeekableInputStream file,
                              List<BlockMetaData> rowGroups,
                              boolean dropColumns) throws IOException {
    for (BlockMetaData block : rowGroups) {
      appendRowGroup(file, block, dropColumns);
    }
  }

  public void appendRowGroup(FSDataInputStream from, BlockMetaData rowGroup,
                             boolean dropColumns) throws IOException {
    appendRowGroup(from, rowGroup, dropColumns);
  }

  public void appendRowGroup(SeekableInputStream from, BlockMetaData rowGroup,
                             boolean dropColumns) throws IOException {
    startBlock(rowGroup.getRowCount());

    Map<String, ColumnChunkMetaData> columnsToCopy =
            new HashMap<String, ColumnChunkMetaData>();
    for (ColumnChunkMetaData chunk : rowGroup.getColumns()) {
      columnsToCopy.put(chunk.getPath().toDotString(), chunk);
    }

    List<ColumnChunkMetaData> columnsInOrder =
            new ArrayList<ColumnChunkMetaData>();

    for (ColumnDescriptor descriptor : schema.getColumns()) {
      String path = ColumnPath.get(descriptor.getPath()).toDotString();
      ColumnChunkMetaData chunk = columnsToCopy.remove(path);
      if (chunk != null) {
        columnsInOrder.add(chunk);
      } else {
        throw new IllegalArgumentException(String.format(
                "Missing column '%s', cannot copy row group: %s", path, rowGroup));
      }
    }

    // complain if some columns would be dropped and that's not okay
    if (!dropColumns && !columnsToCopy.isEmpty()) {
      throw new IllegalArgumentException(String.format(
              "Columns cannot be copied (missing from target schema): %s",
              Strings.join(columnsToCopy.keySet(), ", ")));
    }

    // copy the data for all chunks
    long start = -1;
    long length = 0;
    long blockCompressedSize = 0;
    for (int i = 0; i < columnsInOrder.size(); i += 1) {
      ColumnChunkMetaData chunk = columnsInOrder.get(i);

      // get this chunk's start position in the new file
      long newChunkStart = out.getPos() + length;

      // add this chunk to be copied with any previous chunks
      if (start < 0) {
        // no previous chunk included, start at this chunk's starting pos
        start = chunk.getStartingPos();
      }
      length += chunk.getTotalSize();

      if ((i + 1) == columnsInOrder.size() ||
              columnsInOrder.get(i + 1).getStartingPos() != (start + length)) {
        // not contiguous. do the copy now.
        copy(from, out, start, length);
        // reset to start at the next column chunk
        start = -1;
        length = 0;
      }

      currentBlock.addColumn(ColumnChunkMetaData.get(
              chunk.getPath(),
              chunk.getType(),
              chunk.getCodec(),
              chunk.getEncodingStats(),
              chunk.getEncodings(),
              chunk.getStatistics(),
              newChunkStart,
              newChunkStart,
              chunk.getValueCount(),
              chunk.getTotalSize(),
              chunk.getTotalUncompressedSize()));

      blockCompressedSize += chunk.getTotalSize();
    }

    currentBlock.setTotalByteSize(blockCompressedSize);

    endBlock();
  }

  // Buffers for the copy function.
  private static final ThreadLocal<byte[]> COPY_BUFFER =
          new ThreadLocal<byte[]>() {
            @Override
            protected byte[] initialValue() {
              return new byte[8192];
            }
          };

  /**
   * Copy from a FS input stream to an output stream. Thread-safe
   *
   * @param from a {@link FSDataInputStream}
   * @param to any {@link OutputStream}
   * @param start where in the from stream to start copying
   * @param length the number of bytes to copy
   * @throws IOException
   */
  private static void copy(SeekableInputStream from, FSDataOutputStream to,
                           long start, long length) throws IOException{
    if (DEBUG) LOG.debug(
            "Copying " + length + " bytes at " + start + " to " + to.getPos());
    from.seek(start);
    long bytesCopied = 0;
    byte[] buffer = COPY_BUFFER.get();
    while (bytesCopied < length) {
      long bytesLeft = length - bytesCopied;
      int bytesRead = from.read(buffer, 0,
              (buffer.length < bytesLeft ? buffer.length : (int) bytesLeft));
      if (bytesRead < 0) {
        throw new IllegalArgumentException(
                "Unexpected end of input file at " + start + bytesCopied);
      }
      to.write(buffer, 0, bytesRead);
      bytesCopied += bytesRead;
    }
  }

  /**
   * ends a file once all blocks have been written.
   * closes the file.
   * @param extraMetaData the extra meta data to write in the footer
   * @throws IOException
   */
  public void end(Map<String, String> extraMetaData) throws IOException {
    state = state.end();
    if (DEBUG) LOG.debug(out.getPos() + ": end");
    System.out.println(blocks.size());
    ParquetMetadata footer = new ParquetMetadata(new FileMetaData(schema, extraMetaData, Version.FULL_VERSION), blocks);
    serializeFooter(footer, out);
    out.close();
/*
    Path myPath = new Path(parent, ".test"+this.fileName);
    FileSystem fs = myPath.getFileSystem(configuration);
    FSDataOutputStream myOut = fs.create(myPath,overwriteFlag);
    myOut.write(new byte[]{47});
    myOut.flush();
    myOut.close();
    */
  }

  private static void serializeFooter(ParquetMetadata footer, FSDataOutputStream out) throws IOException {
    long footerIndex = out.getPos();
    org.apache.parquet.format.FileMetaData parquetMetadata = metadataConverter.toParquetMetadata(CURRENT_VERSION, footer);
    writeFileMetaData(parquetMetadata, out);
    long start = out.getPos();
    if(FullBitmapIndex.ON){
      List<BlockMetaData> blocks = footer.getBlocks();
      out.writeInt(blocks.size());
      for (BlockMetaData blockMetaData : blocks) {
        out.writeLong(blockMetaData.getStartingPos());
        blockMetaData.index.serialize(out);
        blockMetaData.index = null;
      }
    }
    if(MDBF.ON){
      List<BlockMetaData> blocks = footer.getBlocks();
      out.writeInt(blocks.size());
      for (BlockMetaData blockMetaData : blocks) {
        out.writeLong(blockMetaData.getStartingPos());
        blockMetaData.mdbfIndex.serialize(out);
        blockMetaData.mdbfIndex = null;
      }
    }
    if(CMDBF.ON){
      List<BlockMetaData> blocks = footer.getBlocks();
      out.writeInt(blocks.size());
      for (BlockMetaData blockMetaData : blocks) {
        out.writeLong(blockMetaData.getStartingPos());
        blockMetaData.cmdbfIndex.serialize(out);
        blockMetaData.cmdbfIndex = null;
      }
    }
    if(FullBitmapIndex.ON || MDBF.ON || CMDBF.ON){
      footer.getFileMetaData().getSchema().getColumns();
      List<ColumnDescriptor> columnList = footer.getFileMetaData().getSchema().getColumns();
      if(RowGroupFilter.checkIndexed(columnList)){
        writeSize(out.getPos()-start);
      }
    }
    if (DEBUG) LOG.debug(out.getPos() + ": footer length = " + (out.getPos() - footerIndex));
    BytesUtils.writeIntLittleEndian(out, (int) (out.getPos() - footerIndex));
    out.write(MAGIC);
  }
  private static void writeSize(long size){
    try {
      File resultFile = new File(RowGroupFilter.filePath+"index-space");
//      if(!resultFile.exists()) resultFile.createNewFile();
      PrintWriter pw = new PrintWriter(new FileWriter(resultFile, true));
      pw.write(size/(1024*1024.0)+" MB.\n");
      pw.flush();
      pw.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Given a list of metadata files, merge them into a single ParquetMetadata
   * Requires that the schemas be compatible, and the extraMetadata be exactly equal.
   */
  public static ParquetMetadata mergeMetadataFiles(List<Path> files,  Configuration conf) throws IOException {
    Preconditions.checkArgument(!files.isEmpty(), "Cannot merge an empty list of metadata");

    GlobalMetaData globalMetaData = null;
    List<BlockMetaData> blocks = new ArrayList<BlockMetaData>();

    for (Path p : files) {
      ParquetMetadata pmd = ParquetFileReader.readFooter(conf, p, ParquetMetadataConverter.NO_FILTER);
      FileMetaData fmd = pmd.getFileMetaData();
      globalMetaData = mergeInto(fmd, globalMetaData, true);
      blocks.addAll(pmd.getBlocks());
    }

    // collapse GlobalMetaData into a single FileMetaData, which will throw if they are not compatible
    return new ParquetMetadata(globalMetaData.merge(), blocks);
  }

  /**
   * Given a list of metadata files, merge them into a single metadata file.
   * Requires that the schemas be compatible, and the extraMetaData be exactly equal.
   * This is useful when merging 2 directories of parquet files into a single directory, as long
   * as both directories were written with compatible schemas and equal extraMetaData.
   */
  public static void writeMergedMetadataFile(List<Path> files, Path outputPath, Configuration conf) throws IOException {
    ParquetMetadata merged = mergeMetadataFiles(files, conf);
    writeMetadataFile(outputPath, merged, outputPath.getFileSystem(conf));
  }

  /**
   * writes a _metadata and _common_metadata file
   * @param configuration the configuration to use to get the FileSystem
   * @param outputPath the directory to write the _metadata file to
   * @param footers the list of footers to merge
   * @deprecated use the variant of writeMetadataFile that takes a {@link JobSummaryLevel} as an argument.
   * @throws IOException
   */
  @Deprecated
  public static void writeMetadataFile(Configuration configuration, Path outputPath, List<Footer> footers) throws IOException {
    writeMetadataFile(configuration, outputPath, footers, JobSummaryLevel.ALL);
  }

  /**
   * writes _common_metadata file, and optionally a _metadata file depending on the {@link JobSummaryLevel} provided
   */
  public static void writeMetadataFile(Configuration configuration, Path outputPath, List<Footer> footers, JobSummaryLevel level) throws IOException {
    Preconditions.checkArgument(level == JobSummaryLevel.ALL || level == JobSummaryLevel.COMMON_ONLY,
            "Unsupported level: " + level);

    FileSystem fs = outputPath.getFileSystem(configuration);
    outputPath = outputPath.makeQualified(fs);
    ParquetMetadata metadataFooter = mergeFooters(outputPath, footers);

    if (level == JobSummaryLevel.ALL) {
      writeMetadataFile(outputPath, metadataFooter, fs, PARQUET_METADATA_FILE);
    }

    metadataFooter.getBlocks().clear();
    writeMetadataFile(outputPath, metadataFooter, fs, PARQUET_COMMON_METADATA_FILE);
  }

  private static void writeMetadataFile(Path outputPathRoot, ParquetMetadata metadataFooter, FileSystem fs, String parquetMetadataFile)
          throws IOException {
    Path metaDataPath = new Path(outputPathRoot, parquetMetadataFile);
    writeMetadataFile(metaDataPath, metadataFooter, fs);
  }

  private static void writeMetadataFile(Path outputPath, ParquetMetadata metadataFooter, FileSystem fs)
          throws IOException {
    FSDataOutputStream metadata = fs.create(outputPath);
    metadata.write(MAGIC);
    serializeFooter(metadataFooter, metadata);
    metadata.close();
  }

  static ParquetMetadata mergeFooters(Path root, List<Footer> footers) {
    String rootPath = root.toUri().getPath();
    GlobalMetaData fileMetaData = null;
    List<BlockMetaData> blocks = new ArrayList<BlockMetaData>();
    for (Footer footer : footers) {
      String footerPath = footer.getFile().toUri().getPath();
      if (!footerPath.startsWith(rootPath)) {
        throw new ParquetEncodingException(footerPath + " invalid: all the files must be contained in the root " + root);
      }
      footerPath = footerPath.substring(rootPath.length());
      while (footerPath.startsWith("/")) {
        footerPath = footerPath.substring(1);
      }
      fileMetaData = mergeInto(footer.getParquetMetadata().getFileMetaData(), fileMetaData);
      for (BlockMetaData block : footer.getParquetMetadata().getBlocks()) {
        block.setPath(footerPath);
        blocks.add(block);
      }
    }
    return new ParquetMetadata(fileMetaData.merge(), blocks);
  }

  /**
   * @return the current position in the underlying file
   * @throws IOException
   */
  public long getPos() throws IOException {
    return out.getPos();
  }

  public long getNextRowGroupSize() throws IOException {
    return alignment.nextRowGroupSize(out);
  }

  /**
   * Will merge the metadata of all the footers together
   * @param footers the list files footers to merge
   * @return the global meta data for all the footers
   */
  static GlobalMetaData getGlobalMetaData(List<Footer> footers) {
    return getGlobalMetaData(footers, true);
  }

  static GlobalMetaData getGlobalMetaData(List<Footer> footers, boolean strict) {
    GlobalMetaData fileMetaData = null;
    for (Footer footer : footers) {
      ParquetMetadata currentMetadata = footer.getParquetMetadata();
      fileMetaData = mergeInto(currentMetadata.getFileMetaData(), fileMetaData, strict);
    }
    return fileMetaData;
  }

  /**
   * Will return the result of merging toMerge into mergedMetadata
   * @param toMerge the metadata toMerge
   * @param mergedMetadata the reference metadata to merge into
   * @return the result of the merge
   */
  static GlobalMetaData mergeInto(
          FileMetaData toMerge,
          GlobalMetaData mergedMetadata) {
    return mergeInto(toMerge, mergedMetadata, true);
  }

  static GlobalMetaData mergeInto(
          FileMetaData toMerge,
          GlobalMetaData mergedMetadata,
          boolean strict) {
    MessageType schema = null;
    Map<String, Set<String>> newKeyValues = new HashMap<String, Set<String>>();
    Set<String> createdBy = new HashSet<String>();
    if (mergedMetadata != null) {
      schema = mergedMetadata.getSchema();
      newKeyValues.putAll(mergedMetadata.getKeyValueMetaData());
      createdBy.addAll(mergedMetadata.getCreatedBy());
    }
    if ((schema == null && toMerge.getSchema() != null)
            || (schema != null && !schema.equals(toMerge.getSchema()))) {
      schema = mergeInto(toMerge.getSchema(), schema, strict);
    }
    for (Entry<String, String> entry : toMerge.getKeyValueMetaData().entrySet()) {
      Set<String> values = newKeyValues.get(entry.getKey());
      if (values == null) {
        values = new LinkedHashSet<String>();
        newKeyValues.put(entry.getKey(), values);
      }
      values.add(entry.getValue());
    }
    createdBy.add(toMerge.getCreatedBy());
    return new GlobalMetaData(
            schema,
            newKeyValues,
            createdBy);
  }

  /**
   * will return the result of merging toMerge into mergedSchema
   * @param toMerge the schema to merge into mergedSchema
   * @param mergedSchema the schema to append the fields to
   * @return the resulting schema
   */
  static MessageType mergeInto(MessageType toMerge, MessageType mergedSchema) {
    return mergeInto(toMerge, mergedSchema, true);
  }

  /**
   * will return the result of merging toMerge into mergedSchema
   * @param toMerge the schema to merge into mergedSchema
   * @param mergedSchema the schema to append the fields to
   * @param strict should schema primitive types match
   * @return the resulting schema
   */
  static MessageType mergeInto(MessageType toMerge, MessageType mergedSchema, boolean strict) {
    if (mergedSchema == null) {
      return toMerge;
    }

    return mergedSchema.union(toMerge, strict);
  }

  private interface AlignmentStrategy {
    void alignForRowGroup(FSDataOutputStream out) throws IOException;

    long nextRowGroupSize(FSDataOutputStream out) throws IOException;
  }

  private static class NoAlignment implements AlignmentStrategy {
    public static NoAlignment get(long rowGroupSize) {
      return new NoAlignment(rowGroupSize);
    }

    private final long rowGroupSize;

    private NoAlignment(long rowGroupSize) {
      this.rowGroupSize = rowGroupSize;
    }

    @Override
    public void alignForRowGroup(FSDataOutputStream out) {
    }

    @Override
    public long nextRowGroupSize(FSDataOutputStream out) {
      return rowGroupSize;
    }
  }

  /**
   * Alignment strategy that pads when less than half the row group size is
   * left before the next DFS block.
   */
  private static class PaddingAlignment implements AlignmentStrategy {
    private static final byte[] zeros = new byte[4096];

    public static PaddingAlignment get(long dfsBlockSize, long rowGroupSize,
                                       int maxPaddingSize) {
      return new PaddingAlignment(dfsBlockSize, rowGroupSize, maxPaddingSize);
    }

    protected final long dfsBlockSize;
    protected final long rowGroupSize;
    protected final int maxPaddingSize;

    private PaddingAlignment(long dfsBlockSize, long rowGroupSize,
                             int maxPaddingSize) {
      this.dfsBlockSize = dfsBlockSize;
      this.rowGroupSize = rowGroupSize;
      this.maxPaddingSize = maxPaddingSize;
    }

    @Override
    public void alignForRowGroup(FSDataOutputStream out) throws IOException {
      long remaining = dfsBlockSize - (out.getPos() % dfsBlockSize);

      if (isPaddingNeeded(remaining)) {
        if (DEBUG) LOG.debug("Adding " + remaining + " bytes of padding (" +
                "row group size=" + rowGroupSize + "B, " +
                "block size=" + dfsBlockSize + "B)");
        for (; remaining > 0; remaining -= zeros.length) {
          out.write(zeros, 0, (int) Math.min((long) zeros.length, remaining));
        }
      }
    }

    @Override
    public long nextRowGroupSize(FSDataOutputStream out) throws IOException {
      if (maxPaddingSize <= 0) {
        return rowGroupSize;
      }

      long remaining = dfsBlockSize - (out.getPos() % dfsBlockSize);

      if (isPaddingNeeded(remaining)) {
        return rowGroupSize;
      }

      return Math.min(remaining, rowGroupSize);
    }

    protected boolean isPaddingNeeded(long remaining) {
      return (remaining <= maxPaddingSize);
    }
  }
}
