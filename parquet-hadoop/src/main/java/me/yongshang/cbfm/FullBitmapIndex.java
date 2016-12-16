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
package me.yongshang.cbfm;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * Created by yongshangwu on 2016/11/8.
 */
public class FullBitmapIndex {
    public static boolean ON = false;

    public static double falsePositiveProbability = 0.1;
    private long predictedCount;
    public static String[] dimensions = new String[]
//            {"p_type", "p_brand", "p_container"};
                {"sip", "dip", "nip"};
    public static String[][] reducedDimensions = new String[][]
//        {{"p_type", "p_container"}};
        {{"dip", "nip"}};

    public static void setDimensions(String[] dimensions, String[][] reducedDimensions){
        FullBitmapIndex.dimensions = dimensions;
        FullBitmapIndex.reducedDimensions = reducedDimensions;
    }

    private String[] reducedStrs;

    private HashMap<String, MultiDBitmapIndex> maps;

    public FullBitmapIndex(DataInput in){
        try {
            predictedCount = in.readLong();

            maps = new HashMap<>();

            int elementCount = in.readInt();
            for (int i = 0; i < elementCount; i++) {
                String key = in.readUTF();
                MultiDBitmapIndex value = new MultiDBitmapIndex(in, falsePositiveProbability, predictedCount);
                maps.put(key, value);
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Init index from input stream failed...");
        }
    }

    public FullBitmapIndex(String str){

        // parse predictedCount
        int countSeparatorIndex = str.indexOf("$");
        predictedCount = Long.valueOf(str.substring(0, countSeparatorIndex));
        str = str.substring(countSeparatorIndex+1);

        maps = new HashMap<>();
        // parse map
        String[] dimensionTokens = str.split("@");
        for (String dimensionToken : dimensionTokens) {
            int separatorIndex = dimensionToken.indexOf(":");
            String key = dimensionToken.substring(0, separatorIndex);
            String value = dimensionToken.substring(separatorIndex+1);
            maps.put(key, new MultiDBitmapIndex(value, falsePositiveProbability, predictedCount));
        }
    }

    public FullBitmapIndex(long predictedCount){
        this.predictedCount = predictedCount;
        setUp();
    }

    public FullBitmapIndex(double fpp,
                           long predictedCount,
                           String[] dimensions,
                           String[][] reducedDimensions){
        this.falsePositiveProbability = fpp;
        this.predictedCount = predictedCount;
        this.dimensions = dimensions;
        this.reducedDimensions = reducedDimensions;

        setUp();
    }

    private void setUp(){
        reducedStrs = new String[reducedDimensions.length];
        for (int i = 0; i < reducedDimensions.length; ++i) {
            String tempReduced = "";
            for (String str : reducedDimensions[i]) {
                tempReduced += (str+"|");
            }
            reducedStrs[i] = tempReduced;
        }
        maps = new HashMap<>();
        for(int len = dimensions.length; len > 0; --len){
            ArrayList<ArrayList<String>> combinations = new ArrayList<>();
            combine(dimensions, 0, len, new ArrayList<String>(), combinations);
            for (ArrayList<String> result : combinations) {
                String currentComb = "";
                for (String str : result) currentComb += (str+"|");
                // check if reduced first
                boolean survive = true;
                for (String reducedStr : reducedStrs) {
                    if(currentComb.contains(reducedStr)){
                        survive = false;
                        break;
                    }
                }
                if(!survive) continue;
                // check if prefix of existent filters
                for (String key : maps.keySet()) {
                    if(key.startsWith(currentComb)){
                        survive = false;
                        break;
                    }
                }
                if(!survive) continue;
                maps.put(currentComb, new MultiDBitmapIndex(falsePositiveProbability, predictedCount, len));
//                System.out.println(currentComb);
            }
        }
    }

    private void combine(String[] arr,
                         int i,
                         int n,
                         ArrayList<String> list,
                         ArrayList<ArrayList<String>> results){
        if (n==0) {
            results.add((ArrayList<String>)list.clone());
            return;
        }
        if (i==arr.length) return;
        list.add(arr[i]);
        combine(arr, i+1, n-1, list, results);
        list.remove(arr[i]);
        combine(arr, i+1, n, list, results);
    }

    public void insert(byte[][] bytes){
        // for each combination indexed
        for (String key : maps.keySet()) {
            String[] columns = key.split("\\|");
            byte[][] indexedBytes = new byte[columns.length][];
            for (int i = 0; i < columns.length; i++) {
                String column = columns[i];
                // find column's order
                int place = 0;
                for(;place<dimensions.length; ++place){
                    if(dimensions[place].equals(column)) break;
                }
                indexedBytes[i] = bytes[place];
            }
            MultiDBitmapIndex index = maps.get(key);
            index.insert(indexedBytes);
            /*
            System.out.println("Insert into key: "+key+" with value: ");
            for(int i = 0; i < columns.length; ++i){
                System.out.println("\t"+columns[i]+": "+Arrays.toString(indexedBytes[i]));
            }
            */
        }
    }

    public boolean contains(String[] columns, byte[][] bytes){
        String comb = "";
        for (String column : columns) {
            comb += (column+"|");
        }
        for (String key : maps.keySet()) {
            if(key.startsWith(comb)){
                return maps.get(key).contains(bytes);
            }
        }
        // if combination not indexed, give possibly false positive result.
        return true;
    }

    public void displayUsage(){
        long[] usage = new long[2];
        for (String key : maps.keySet()) {
            long[] tempUsage = maps.get(key).getUsage();
            usage[0] += (tempUsage[0] + key.getBytes().length);
            usage[1] += (tempUsage[1] + key.getBytes().length);
        }
        System.out.println("[FullBitmapIdx]\tin memory size: "+usage[0]/(1024.0*1024)+"MB," +
                " compressed: "+usage[1]/(1024.0*1024)+"MB");
    }

    public String compress(){
        StringBuilder sb = new StringBuilder();
        sb.append(predictedCount+"$");
        Set<String> keySet = new HashSet<>();
        for (String key : maps.keySet()) {
            keySet.add(key);
        }
        for (String key : keySet) {
            sb.append(key+":");
            MultiDBitmapIndex index = maps.get(key);
            sb.append(index.compress());
            maps.remove(key);// save memory
            sb.append("@");
        }
        return sb.toString();
    }

    public void serialize(DataOutput out) throws IOException {
        out.writeLong(predictedCount);

        Set<String> keySet = maps.keySet();
        out.writeInt(keySet.size());

        for (String key : keySet) {
            out.writeUTF(key);
            MultiDBitmapIndex index = maps.get(key);
            index.serialize(out);
        }
    }

    public void optimize(){
        for (String key : maps.keySet()) {
            maps.get(key).optimize();
        }
    }
}
