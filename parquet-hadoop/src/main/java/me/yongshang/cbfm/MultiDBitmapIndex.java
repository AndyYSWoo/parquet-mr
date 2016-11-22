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

import org.roaringbitmap.RoaringBitmap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Random;

/**
 * Created by yongshangwu on 2016/11/8.
 */
public class MultiDBitmapIndex {
    private static final int PREDEF_SALT_COUNT = 128;	//原始种子数量（用于计算每个哈希采用的种子）

    private double falsePositiveProbability;
    private long predictedCount;

    private int saltCount;
    private long[] salts;

    private int m;

    private int dimension;

    private UnifiedMap map;

    public MultiDBitmapIndex(DataInput in, double fpp, long predictedCount){
        this.falsePositiveProbability = fpp;
        this.predictedCount = predictedCount;
        initParams();
        generateSalts();

        map = new UnifiedMap(in);
    }

    public MultiDBitmapIndex(String str, double fpp, long predictedCount){
        this.falsePositiveProbability = fpp;
        this.predictedCount = predictedCount;
        initParams();
        generateSalts();

        map = new UnifiedMap(str);

    }

    public MultiDBitmapIndex(double fpp, long predictedCount, int dimension){
        this.falsePositiveProbability = fpp;
        this.predictedCount = predictedCount;
        initParams();
        generateSalts();
        this.dimension = dimension;
        map = new UnifiedMap(dimension==1);
    }

    private void initParams(){
        // function count k
        double f = this.falsePositiveProbability;
        int k = (int) Math.floor(-Math.log(f) / Math.log(2)); // k = -log2(f)
        this.saltCount = k;
        this.salts = new long[saltCount];
        // decide m
        long n = predictedCount;
        this.m = (int) Math.ceil(n * (1/Math.log(2)) * (Math.log(1/f)/Math.log(2)));
    }

    private void generateSalts(){
        		/*
		  Note:
		  A distinct hash function need not be implementation-wise
		  distinct. In the current implementation "seeding" a common
		  hash function with different values seems to be adequate.
		*/
        final long[] predef_salt = {
                0xAAAAAAAAAAAAAAAAL, 0x5555555555555555L, 0x3333333333333333L, 0xCCCCCCCCCCCCCCCCL,
                0x6666666666666666L, 0x9999999999999999L, 0xB5B5B5B5B5B5B5B5L, 0x4B4B4B4B4B4B4B4BL,
                0xAA55AA55AA55AA55L, 0x5533553355335533L, 0x33CC33CC33CC33CCL, 0xCC66CC66CC66CC66L,
                0x6699669966996699L, 0x99B599B599B599B5L, 0xB54BB54BB54BB54BL, 0x4BAA4BAA4BAA4BAAL,
                0xAA33AA33AA33AA33L, 0x55CC55CC55CC55CCL, 0x3366336633663366L, 0xCC99CC99CC99CC99L,
                0x66B566B566B566B5L, 0x994B994B994B994BL, 0xB5AAB5AAB5AAB5AAL, 0xAAAAAAAAAAAAAA33L,
                0x55555555555555CCL, 0x3333333333333366L, 0xCCCCCCCCCCCCCC99L, 0x66666666666666B5L,
                0x999999999999994BL, 0xB5B5B5B5B5B5B5AAL, 0xFFFFFFFFFFFFFFFFL, 0xFFFFFFFF00000000L,
                0xB823D5EBB823D5EBL, 0xC1191CDFC1191CDFL, 0xF623AEB3F623AEB3L, 0xDB58499FDB58499FL,
                0xC8D42E70C8D42E70L, 0xB173F616B173F616L, 0xA91A5967A91A5967L, 0xDA427D63DA427D63L,
                0xB1E8A2EAB1E8A2EAL, 0xF6C0D155F6C0D155L, 0x4909FEA34909FEA3L, 0xA68CC6A7A68CC6A7L,
                0xC395E782C395E782L, 0xA26057EBA26057EBL, 0x0CD5DA280CD5DA28L, 0x467C5492467C5492L,
                0xF15E6982F15E6982L, 0x61C6FAD361C6FAD3L, 0x9615E3529615E352L, 0x6E9E355A6E9E355AL,
                0x689B563E689B563EL, 0x0C9831A80C9831A8L, 0x6753C18B6753C18BL, 0xA622689BA622689BL,
                0x8CA63C478CA63C47L, 0x42CC288442CC2884L, 0x8E89919B8E89919BL, 0x6EDBD7D36EDBD7D3L,
                0x15B6796C15B6796CL, 0x1D6FDFE41D6FDFE4L, 0x63FF909263FF9092L, 0xE7401432E7401432L,
                0xEFFE9412EFFE9412L, 0xAEAEDF79AEAEDF79L, 0x9F245A319F245A31L, 0x83C136FC83C136FCL,
                0xC3DA4A8CC3DA4A8CL, 0xA5112C8CA5112C8CL, 0x5271F4915271F491L, 0x9A948DAB9A948DABL,
                0xCEE59A8DCEE59A8DL, 0xB5F525ABB5F525ABL, 0x59D1321759D13217L, 0x24E7C33124E7C331L,
                0x697C2103697C2103L, 0x84B0A46084B0A460L, 0x86156DA986156DA9L, 0xAEF2AC68AEF2AC68L,
                0x23243DA523243DA5L, 0x3F6496433F649643L, 0x5FA495A85FA495A8L, 0x67710DF867710DF8L,
                0x9A6C499E9A6C499EL, 0xDCFB0227DCFB0227L, 0x46A4343346A43433L, 0x1832B07A1832B07AL,
                0xC46AFF3CC46AFF3CL, 0xB9C8FFF0B9C8FFF0L, 0xC9500467C9500467L, 0x34431BDF34431BDFL,
                0xB652432BB652432BL, 0xE367F12BE367F12BL, 0x427F4C1B427F4C1BL, 0x224C006E224C006EL,
                0x2E7E5A892E7E5A89L, 0x96F99AA596F99AA5L, 0x0BEB452A0BEB452AL, 0x2FD87C392FD87C39L,
                0x74B2E1FB74B2E1FBL, 0x222EFD24222EFD24L, 0xF357F60CF357F60CL, 0x440FCB1E440FCB1EL,
                0x8BBE030F8BBE030FL, 0x6704DC296704DC29L, 0x1144D12F1144D12FL, 0x948B1355948B1355L,
                0x6D8FD7E96D8FD7E9L, 0x1C11A0141C11A014L, 0xADD1592FADD1592FL, 0xFB3C712EFB3C712EL,
                0xFC77642FFC77642FL, 0xF9C4CE8CF9C4CE8CL, 0x31312FB931312FB9L, 0x08B0DD7908B0DD79L,
                0x318FA6E7318FA6E7L, 0xC040D23DC040D23DL, 0xC0589AA7C0589AA7L, 0x0CA5C0750CA5C075L,
                0xF874B172F874B172L, 0x0CF914D50CF914D5L, 0x784D3280784D3280L, 0x4E8CFEBC4E8CFEBCL,
                0xC569F575C569F575L, 0xCDB2A091CDB2A091L, 0x2CC016B42CC016B4L, 0x5C5F44215C5F4421L
        };

        if (saltCount <= PREDEF_SALT_COUNT) {
            for (int j = 0; j < saltCount; j++) {
                salts[j] = predef_salt[j];
            }
            for(int i = 0; i < saltCount; ++i)
            {
				/*
				  Note:
				  This is done to integrate the user defined random seed,
				  so as to allow for the generation of unique bloom filter
				  instances.
				*/
                salts[i] = salts[i] * salts[(i + 3) % saltCount];
            }
        }
        else
        {
            int i = 0;
            int j = 0;
            for (int j2 = 0; j2 < PREDEF_SALT_COUNT; j2++) {
                salts[j2] = predef_salt[j2];
            }
            Random random = new Random();
            while(i < PREDEF_SALT_COUNT) {
                long current_salt = random.nextLong() * random.nextLong();
                if (0 == current_salt) {
                    continue;
                }
                boolean found = false;
                for(j = 0; j < i; j++) {
                    if(current_salt == salts[j]) {
                        found = true;
                        break;
                    }
                }
                if(!found) {
                    salts[i] = current_salt;
                    i++;
                }
            }
        }
    }

    public void insert(byte[][] bytes){
        int[][] indexes = new int[bytes.length][];
        for (int i = 0; i < bytes.length; i++) {
            long[] hash = hash(bytes[i]);
            indexes[i] = computeIndexes(hash);
        }
        addBitsToMap(map, 0, indexes);
    }

    /**
     * Add indexes[level+1] to map, recursively
     * @param map
     * @param level
     * @param indexes
     */
    private void addBitsToMap(UnifiedMap map, int level, int[][] indexes){
        if(!map.isLast()){
            HashMap<Integer, UnifiedMap> higherMap = map.getMidMap();
            for (int idx : indexes[level]) {
                if(!higherMap.containsKey(idx)){
                    higherMap.put(idx, new UnifiedMap(level == dimension-2));
                }
                addBitsToMap(higherMap.get(idx), level+1, indexes);
            }
        }else{
            RoaringBitmap bitmap = map.getBitmap();
            for (int idx : indexes[level]) {
                bitmap.add(idx);
            }
        }
    }

    public boolean contains(byte[][] bytes){
        int[][] indexes = new int[bytes.length][];
        for (int i = 0; i < bytes.length; i++) {
            long[] hash = hash(bytes[i]);
            indexes[i] = computeIndexes(hash);
        }
        return findBitsInMap(map, 0, indexes);
    }

    private boolean findBitsInMap(UnifiedMap map, int level, int[][] indexes){
        if(level >= indexes.length || indexes[level] == null) return true;
        if(!map.isLast()){
            HashMap<Integer, UnifiedMap> higherMap = map.getMidMap();
            for (int idx : indexes[level]) {
                if(!higherMap.containsKey(idx)){
                    // find a bit not set this level
                    return false;
                }else{
                    if(!findBitsInMap(higherMap.get(idx), level+1, indexes)){
                        // not found next level
                        return false;
                    }
                }
            }
            return true;
        }else{
            RoaringBitmap bitmap = map.getBitmap();
            for (int bit : indexes[level]) {
                if(!bitmap.contains(bit)) return false;
            }
            return true;
        }
    }

    private long[] hash(byte[] element){
        if(element == null) return null;
        long[] hashes = new long[saltCount];
        for(int i = 0; i < saltCount; ++i){
            long hash = salts[i];
            int remaining_length = element.length;
            int j = 0;
            while(remaining_length >= 2) {
                hash ^=    (hash <<  7) ^  (element[j++]) * (hash >>> 3);
                hash ^= (~((hash << 11) + ((element[j++]) ^ (hash >>> 5))));
                remaining_length -= 2;
            }
            if (0 != remaining_length) {
                hash ^= (hash <<  7) ^ (element[j]) * (hash >>> 3);
            }
            hashes[i] = hash;
        }
        return hashes;
    }

    private int[] computeIndexes(long[] hashes){
        if(hashes == null) return null;
        int[] indexes = new int[hashes.length];
        for (int i = 0; i < hashes.length; i++) {
            indexes[i] = (int)((hashes[i] & 0x7fffffffffffffffL) % m);
        }
        return indexes;
    }

    private long sizeInMem;
    private long sizeCompressed;
    public long[] getUsage(){
        long[] usage = new long[2];

        sizeInMem = 0;
        sizeCompressed = 0;
        collectSize(map);
        usage[0] = sizeInMem;
        usage[1] = sizeCompressed;
        return usage;
    }

    private void collectSize(UnifiedMap map){
        if(map.isLast()){
            RoaringBitmap bitmap = map.getBitmap();
            sizeInMem += bitmap.getSizeInBytes();
            bitmap.runOptimize();
            sizeCompressed += bitmap.getSizeInBytes();
        }else{
            HashMap<Integer, UnifiedMap> highMap = map.getMidMap();
            for (Integer idx : highMap.keySet()) {
                sizeInMem += 4;
                sizeCompressed += 4;
                collectSize(highMap.get(idx));
            }
        }
    }

    public void displayUsage(){
        System.out.println("[MDBitmapIdx]\tk: "+saltCount);
        System.out.println("[MDBitmapIdx]\tm: "+m);

        long[] usage = getUsage();
        System.out.println("[MDBitmapIdx]\tin memory size: "+usage[0]/(1024.0*1024)+"MB," +
                " compressed: "+usage[1]/(1024.0*1024)+"MB");
    }

    public String compress(){
        return map.compress();
    }

    public void serialize(DataOutput out) throws IOException {
        map.serialize(out);
    }

    public void optimize(){
        map.optimize();
    }
}
