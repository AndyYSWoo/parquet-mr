package me.yongshang.cbfm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

/**
 * Created by yongshangwu on 2016/11/8.
 */
public class FullBitmapIndex {


    private double falsePositiveProbability;
    private long predictedCount;
    private String[] dimensions;
    private String[][] reducedDimensions;
    private String[] reducedStrs;

    private HashMap<String, MultiDBitmapIndex> maps;

    public FullBitmapIndex(double fpp,
                           long predictedCount,
                           String[] dimensions,
                           String[][] reducedDimensions){
        this.falsePositiveProbability = fpp;
        this.predictedCount = predictedCount;
        this.dimensions = dimensions;
        this.reducedDimensions = reducedDimensions;

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
            if(key.equals(comb)){
                return maps.get(key).contains(bytes);
            }
        }
        // if combination not indexed, give possibaly false positive result.
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
}
