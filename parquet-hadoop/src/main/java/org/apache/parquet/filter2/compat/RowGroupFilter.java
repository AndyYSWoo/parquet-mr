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

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import me.yongshang.cbfm.CBFM;
import me.yongshang.cbfm.FullBitmapIndex;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.filter2.compat.FilterCompat.NoOpFilter;
import org.apache.parquet.filter2.compat.FilterCompat.Visitor;
import org.apache.parquet.filter2.dictionarylevel.DictionaryFilter;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.filter2.predicate.SchemaCompatibilityValidator;
import org.apache.parquet.filter2.statisticslevel.StatisticsFilter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.TaskContext$;
import org.apache.spark.deploy.SparkHadoopUtil;

import static org.apache.parquet.Preconditions.checkNotNull;

/**
 * Given a {@link Filter} applies it to a list of BlockMetaData (row groups)
 * If the Filter is an {@link org.apache.parquet.filter.UnboundRecordFilter} or the no op filter,
 * no filtering will be performed.
 */
public class RowGroupFilter implements Visitor<List<BlockMetaData>> {
  private final List<BlockMetaData> blocks;
  private final MessageType schema;
  private final List<FilterLevel> levels;
  private final ParquetFileReader reader;
  public static String filePath;

  public enum FilterLevel {
    STATISTICS,
    DICTIONARY
  }

  public static List<BlockMetaData> filterRowGroupsByCBFM(Filter filter, List<BlockMetaData> blocks, MessageType schema){
    if(!(CBFM.ON || FullBitmapIndex.ON)) return blocks;
    // Only applying filters on indexed table
    if(CBFM.ON && blocks.get(0).getIndexTableStr() == null) return blocks;
    if(FullBitmapIndex.ON && blocks.get(0).index == null) return blocks;
    List<BlockMetaData> cadidateBlocks = new ArrayList<>();
    if(filter instanceof FilterCompat.FilterPredicateCompat){
      // only deal with FilterPredicateCompat
      FilterCompat.FilterPredicateCompat filterPredicateCompat = (FilterCompat.FilterPredicateCompat) filter;
      FilterPredicate filterPredicate = filterPredicateCompat.getFilterPredicate();
      List<Operators.Eq> eqFilters = new ArrayList<>();
      extractEqFilter(filterPredicate, eqFilters);

      String[] indexedColumns = null;
      if(CBFM.ON) indexedColumns = CBFM.indexedColumns;
      else if(FullBitmapIndex.ON) indexedColumns = FullBitmapIndex.dimensions;

      String[] currentComb = new String[eqFilters.size()];
      byte[][] indexedColumnBytes = new byte[indexedColumns.length][];
      for(int j = 0; j < eqFilters.size(); ++j){
        Operators.Eq eqFilter = eqFilters.get(j);

        String[] columnPath = eqFilter.getColumn().getColumnPath().toArray();
        String columnName = columnPath[columnPath.length-1];
        currentComb[j] = columnName;

        for(int i = 0; i < indexedColumns.length; ++i){
          if(indexedColumns[i].equals(columnName)){
            Comparable value = eqFilter.getValue();
            if(value instanceof Binary){
              indexedColumnBytes[i] = ((Binary) value).getBytes();
            }else if(value instanceof Integer){
              indexedColumnBytes[i] = ByteBuffer.allocate(4).putInt((Integer) value).array();
              ArrayUtils.reverse(indexedColumnBytes[i]);
            }else if(value instanceof Long){
              indexedColumnBytes[i] = ByteBuffer.allocate(8).putLong((Long) value).array();
              ArrayUtils.reverse(indexedColumnBytes[i]);
            }else if(value instanceof Float){
              indexedColumnBytes[i] = ByteBuffer.allocate(4).putFloat((Float) value).array();
              ArrayUtils.reverse(indexedColumnBytes[i]);
            }else if(value instanceof Double){
              indexedColumnBytes[i] = ByteBuffer.allocate(8).putDouble((Double)value).array();
              ArrayUtils.reverse(indexedColumnBytes[i]);
            }
          }
        }
      }
      int hitCount = 0;
      for (BlockMetaData block : blocks) {
        if(CBFM.ON) {
          try {
            Path cbfmFile = new Path(block.getIndexTableStr());
            FileSystem fs = cbfmFile.getFileSystem(new Configuration());
            // TODO better way? Or is this right? escape the temp folder
            cbfmFile = new Path(cbfmFile.getParent().getParent().getParent().getParent().getParent(), cbfmFile.getName());
            FSDataInputStream in = fs.open(cbfmFile);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String indexTableStr = br.readLine();
            br.close();
            in.close();
            in = null;
            br.close();
            br = null;
            CBFM cbfm = new CBFM(indexTableStr);
            ArrayList<Long> searchIndex = cbfm.calculateIdxsForSearch(indexedColumnBytes);
            if (cbfm.contains(searchIndex)) {
              hitCount++;
              cadidateBlocks.add(block);
            }
          } catch (IOException e) {
            e.printStackTrace();
          }
        }else if(FullBitmapIndex.ON){
          FullBitmapIndex index = block.index;
          if(index.contains(currentComb, indexedColumnBytes)){
            hitCount++;
            cadidateBlocks.add(block);
          }
        }
      }
      int skippedCount = blocks.size() - hitCount;
      writeSkipResults(skippedCount, blocks.size());
    }
    return cadidateBlocks;
  }

  private static void writeSkipResults(int skippedCount, int totalCount){
    try {
      File resultFile = new File(filePath);
      PrintWriter pw = new PrintWriter(new FileWriter(resultFile, true));
      pw.write("Task "+TaskContext.get().taskAttemptId()+": total "+totalCount+" blocks, "+skippedCount+" blocks skipped.\n");
      pw.flush();
      pw.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static void extractEqFilter(FilterPredicate filterPredicate, List<Operators.Eq> list){
    if(filterPredicate instanceof Operators.And){
      Operators.And andFilter = (Operators.And) filterPredicate;
      extractEqFilter(andFilter.getLeft(), list);
      extractEqFilter(andFilter.getRight(), list);
    }else if(filterPredicate instanceof Operators.Eq){
      list.add((Operators.Eq)filterPredicate);
    }
  }

  public static List<BlockMetaData> filterRowGroups(Filter filter, List<BlockMetaData> blocks, MessageType schema) {
    checkNotNull(filter, "filter");
    return filter.accept(new RowGroupFilter(blocks, schema));
  }

  public static List<BlockMetaData> filterRowGroups(List<FilterLevel> levels, Filter filter, List<BlockMetaData> blocks, ParquetFileReader reader) {
    checkNotNull(filter, "filter");
    return filter.accept(new RowGroupFilter(levels, blocks, reader));
  }

  @Deprecated
  private RowGroupFilter(List<BlockMetaData> blocks, MessageType schema) {
    this.blocks = checkNotNull(blocks, "blocks");
    this.schema = checkNotNull(schema, "schema");
    this.levels = Collections.singletonList(FilterLevel.STATISTICS);
    this.reader = null;
  }

  private RowGroupFilter(List<FilterLevel> levels, List<BlockMetaData> blocks, ParquetFileReader reader) {
    this.blocks = checkNotNull(blocks, "blocks");
    this.reader = checkNotNull(reader, "reader");
    this.schema = reader.getFileMetaData().getSchema();
    this.levels = levels;
  }

  @Override
  public List<BlockMetaData> visit(FilterCompat.FilterPredicateCompat filterPredicateCompat) {
    FilterPredicate filterPredicate = filterPredicateCompat.getFilterPredicate();

    // check that the schema of the filter matches the schema of the file
    SchemaCompatibilityValidator.validate(filterPredicate, schema);

    List<BlockMetaData> filteredBlocks = new ArrayList<BlockMetaData>();

    for (BlockMetaData block : blocks) {
      boolean drop = false;

      if(levels.contains(FilterLevel.STATISTICS)) {
        drop = StatisticsFilter.canDrop(filterPredicate, block.getColumns());
      }

      if(!drop && levels.contains(FilterLevel.DICTIONARY)) {
        drop = DictionaryFilter.canDrop(filterPredicate, block.getColumns(), reader.getDictionaryReader(block));
      }

      if(!drop) {
        filteredBlocks.add(block);
      }
    }

    return filteredBlocks;
  }

  @Override
  public List<BlockMetaData> visit(FilterCompat.UnboundRecordFilterCompat unboundRecordFilterCompat) {
    return blocks;
  }

  @Override
  public List<BlockMetaData> visit(NoOpFilter noOpFilter) {
    return blocks;
  }
}
