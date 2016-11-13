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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;

/**
 * Created by yongshangwu on 2016/11/8.
 */
public class UnifiedMap {
    private boolean isLast;
    private HashMap<Integer, UnifiedMap> midMap;
    private RoaringBitmap bitmap;

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

    public String compress(){
        StringBuilder sb = new StringBuilder();
        if(isLast){
            ByteBuffer byteBuffer = ByteBuffer.allocate(bitmap.serializedSizeInBytes());
            try {
                bitmap.serialize(new DataOutputStream(new OutputStream() {
                    ByteBuffer buffer;
                    OutputStream init(ByteBuffer buffer){
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
                }));
            } catch (IOException e) {
                e.printStackTrace();
            }
            sb.append(Arrays.toString(byteBuffer.array()));
        }else{
            for (Integer bit : midMap.keySet()) {
                sb.append(bit +":"+ midMap.get(bit).compress());
            }
        }
        return null;
    }
}
