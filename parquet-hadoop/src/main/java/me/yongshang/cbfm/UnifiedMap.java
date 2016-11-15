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

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by yongshangwu on 2016/11/8.
 */
public class UnifiedMap {
    private boolean isLast;
    private HashMap<Integer, UnifiedMap> midMap;
    private RoaringBitmap bitmap;

    public UnifiedMap(DataInput in){
        try {
            char id = in.readChar();
            if(id == 'B'){
                isLast = true;
                bitmap = new RoaringBitmap();
                bitmap.deserialize(in);
            }else{
                isLast = false;
                int elementCount = in.readInt();
                midMap = new HashMap<>(elementCount);
                for (int i = 0; i < elementCount; i++) {
                    int key = in.readInt();
                    UnifiedMap value = new UnifiedMap(in);
                    midMap.put(key, value);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Init UnifiedMap from DataInput failed...");
        }
    }

    public UnifiedMap(String str){
        if(str.charAt(0) == 'B'){
            isLast = true;
            str = str.substring(2, str.length()-1);
            String[] bitTokens = str.split(", ");
            byte[] bytes = new byte[bitTokens.length];
            for (int i = 0; i < bitTokens.length; i++) {
                bytes[i] = Byte.valueOf(bitTokens[i]);
            }
            bitmap = new RoaringBitmap();
            try {
                bitmap.deserialize(new DataInputStream(new ByteArrayInputStream(bytes)));
            } catch (IOException e) {
                System.err.println("Init RoaringBitmap from string failed...");
                e.printStackTrace();
            }
        }else{
            isLast = false;
            midMap = new HashMap<>();
            str = str.substring(1);
            String[] entryTokens = str.split("%");
            for (String entryToken : entryTokens) {
                if(entryToken.length() == 0) continue;
                int separatorIndex = entryToken.indexOf(":");
                int key = Integer.valueOf(entryToken.substring(0, separatorIndex));
                midMap.put(key, new UnifiedMap(entryToken.substring(separatorIndex+1)));
            }
        }
    }

    public UnifiedMap(boolean isLast){
        setLast(isLast);
        if(isLast){
            bitmap = new RoaringBitmap();
        }else{
            midMap = new HashMap<>();
        }
    }

    public boolean isLast() {
        return isLast;
    }

    public void setLast(boolean last) {
        isLast = last;
    }

    public HashMap<Integer, UnifiedMap> getMidMap() {
        return midMap;
    }

    public void setMidMap(HashMap<Integer, UnifiedMap> midMap) {
        this.midMap = midMap;
    }

    public RoaringBitmap getBitmap() {
        return bitmap;
    }

    public void setBitmap(RoaringBitmap bitmap) {
        this.bitmap = bitmap;
    }
    // TODO For now only support 2 layer embeded, if to extend, change separator
    public String compress(){
        StringBuilder sb = new StringBuilder();
        if(isLast){
            ByteBuffer byteBuffer = ByteBuffer.allocate(bitmap.serializedSizeInBytes());
            try {
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
            } catch (IOException e) {
                e.printStackTrace();
            }
            // save memory
            bitmap.clear();
            bitmap = null;

            sb.append("B"+Arrays.toString(byteBuffer.array()));

        }else{
            sb.append("H");
            Set<Integer> keySet = new HashSet<>();
            for (Integer bit : midMap.keySet()) {
                keySet.add(bit);
            }
            for (Integer bit : keySet) {
                sb.append(bit +":"+ midMap.get(bit).compress()+"%");
                midMap.remove(bit);// save memory
            }
        }
        return sb.toString();
    }

    public void serialize(DataOutput out) throws IOException {
        if(isLast){
            out.writeChar('B');
            bitmap.serialize(out);
        }else{
            out.writeChar('H');
            Set<Integer> keySet = midMap.keySet();
            out.writeInt(keySet.size());
            for (Integer key : keySet) {
                out.writeInt(key);
                midMap.get(key).serialize(out);
            }
        }
    }
}
