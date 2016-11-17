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

/**
 * Created by yongshangwu on 2016/11/17.
 */
public class Util {
    static final int BITS_PER_LONG = 64;
    static final int PREDEF_SALT_COUNT = 128;
    public static final long[] bit_mask = {
            0x0000000000000001L,
            0x0000000000000002L,
            0x0000000000000004L,
            0x0000000000000008L,
            0x0000000000000010L,
            0x0000000000000020L,
            0x0000000000000040L,
            0x0000000000000080L,
            0x0000000000000100L,
            0x0000000000000200L,
            0x0000000000000400L,
            0x0000000000000800L,
            0x0000000000001000L,
            0x0000000000002000L,
            0x0000000000004000L,
            0x0000000000008000L,
            0x0000000000010000L,
            0x0000000000020000L,
            0x0000000000040000L,
            0x0000000000080000L,
            0x0000000000100000L,
            0x0000000000200000L,
            0x0000000000400000L,
            0x0000000000800000L,
            0x0000000001000000L,
            0x0000000002000000L,
            0x0000000004000000L,
            0x0000000008000000L,
            0x0000000010000000L,
            0x0000000020000000L,
            0x0000000040000000L,
            0x0000000080000000L,
            0x0000000100000000L,
            0x0000000200000000L,
            0x0000000400000000L,
            0x0000000800000000L,
            0x0000001000000000L,
            0x0000002000000000L,
            0x0000004000000000L,
            0x0000008000000000L,
            0x0000010000000000L,
            0x0000020000000000L,
            0x0000040000000000L,
            0x0000080000000000L,
            0x0000100000000000L,
            0x0000200000000000L,
            0x0000400000000000L,
            0x0000800000000000L,
            0x0001000000000000L,
            0x0002000000000000L,
            0x0004000000000000L,
            0x0008000000000000L,
            0x0010000000000000L,
            0x0020000000000000L,
            0x0040000000000000L,
            0x0080000000000000L,
            0x0100000000000000L,
            0x0200000000000000L,
            0x0400000000000000L,
            0x0800000000000000L,
            0x1000000000000000L,
            0x2000000000000000L,
            0x4000000000000000L,
            0x8000000000000000L,
    };
}
