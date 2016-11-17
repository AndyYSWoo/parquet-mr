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

import java.io.Serializable;
import java.util.Random;

public class CBF implements Serializable {
    private static final long serialVersionUID = 1L;
    static final long[] bit_mask = {
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
    static final long BITS_PER_LONG = 64;
    static final int PREDEF_SALT_COUNT = 128;
    //通过运行c语言的bloomfilter，种子设置为1000时得到如下hash运算的初始值
    long[] salt_ = null;
    long[] bit_table_;
    long[] predicted_element_counts_;        //期望元素个数
    double[] desired_false_positive_probabilitys_;   //期望错误率
    double desired_false_positive_probability_;   //错误率
    int[] salt_counts_;                     //hash函数个数
    long[] table_sizes_;                     //位数组大小（bit单位）
    long[] idxMultis;
    int dimension;


    public CBF(long[] predicted_element_count, double[] desired_false_positive_probability, int dimension)
    {
//		predicted_element_counts_ = new long[dimension];
//		desired_false_positive_probabilitys_ = new double[dimension];
//		System.arraycopy(predicted_element_count, 0, predicted_element_counts_, 0, dimension);
//		System.arraycopy(desired_false_positive_probability, 0, desired_false_positive_probabilitys_, 0, dimension);
        this.dimension = dimension;

        table_sizes_ = new long[dimension];
        salt_counts_ = new int[dimension];
        idxMultis = new long[dimension];
        //确定salt_count_ 和table_size_ 的大小
//		bloom_filter_find_optimal_parameters();

        for (int i = 0; i < dimension; i++) {
            table_sizes_[i] = 513l;//(long)Math.scalb(1.0, 27/dimension);
            salt_counts_[i] = 6;
        }


        for (int i = 0; i < dimension; i++) {
            idxMultis[i] = 1l;
            for (int j = 0; j < i; j++) {
                idxMultis[i] *= table_sizes_[j];
            }
        }

        int maxsaltNum = Integer.MIN_VALUE;
        for (int i : salt_counts_) {
            if (i > maxsaltNum) {
                maxsaltNum = i;
            }
        }
        salt_ = new long [maxsaltNum];
        //选种子
        bloom_filter_generate_unique_salt();
        //由于索引一定要使用int，加上java不支持无符整型，
        //若使用char或byte数组，长度会超过int的最大值，因此使用long数组

        long totalBitSize = 1;
        for (int i = 0; i < dimension; i++) {
            totalBitSize *= table_sizes_[i];
        }
        int longLen = (int)(((totalBitSize + BITS_PER_LONG - 1) / BITS_PER_LONG));
        bit_table_ = new long[longLen];
    }


    public void bloom_filter_insert(byte[][] keys)
    {
        long[][] totalIdx = new long[salt_.length][dimension];
        int curD = 0;
        for (byte[] key : keys) {
            //由于java中没有指针，long也无法按引用传值，因此使用长度为1的数组
            long[] bit_index = {0};
            long idxMulti = 1l;
            for (int j = 0; j < curD; j++) {
                idxMulti *= table_sizes_[j];
            }
            for(int i = 0; i < salt_.length; ++i)
            {
                bloom_filter_compute_indices(
                        bloom_filter_hash_ap(key, key.length, salt_[i]),
                        table_sizes_[curD], bit_index);
                long singleIdx = idxMulti * bit_index[0];
                totalIdx[i][curD] = singleIdx;
            }
            curD++;
        }
        for (long[] ls : totalIdx) {
            markCombine(ls, 0, dimension, 0l);
        }
    }

    private void markCombine(long[] totalIdx, int start, int num, long idxOffset) {
        for (int i = start; i < totalIdx.length; i++) {
            long first = totalIdx[i];
            if (1 == num) {
                bit_table_[(int)((idxOffset + first) / BITS_PER_LONG)] |= bit_mask[(int)((idxOffset + first) % BITS_PER_LONG)];
            } else {
                for (int j = i + 1; j < totalIdx.length; j++) {
                    markCombine(totalIdx, j, num - 1, idxOffset + first);
                }
            }
        }
    }

    /*
     * check the element exist or not in bf
     * return:
     *          1  exist
     *          0  not exist
     */
    public boolean bloom_filter_contains(byte[][] keys)
    {
        long[][] totalIdx = new long[salt_.length][dimension];
        int curD = 0;

        for (byte[] key : keys) {
            if (null == key) {
                curD++;
                continue;
            }
            //由于java中没有指针，long也无法按引用传值，因此使用长度为1的数组
            long[] bit_index = {0};
            for(int i = 0; i < salt_.length; ++i)
            {
                bloom_filter_compute_indices(
                        bloom_filter_hash_ap(key, key.length, salt_[i]),
                        table_sizes_[curD], bit_index);
                long singleIdx = idxMultis[curD] * bit_index[0];
                totalIdx[i][curD] = singleIdx;
            }
            curD++;
        }
        for (long[] ls : totalIdx) {
            if (!addIdxAndJudgeForSearch(keys, ls, idxMultis, 0, 0l)) {
                return false;
            }
        }
        return true;
    }

    boolean addIdxAndJudgeForSearch(byte[][] keys, long[] ls, long[] idxMultis, int toAddDimension, long offsetIdx) {
        if (toAddDimension >= dimension) {
            return 0 != (bit_table_[(int) (offsetIdx / BITS_PER_LONG)] & bit_mask[(int) (offsetIdx % BITS_PER_LONG)]);
        }
        if (null == keys[toAddDimension]) {
            for (long i = 0; i < table_sizes_[toAddDimension]; i++) {
                if (addIdxAndJudgeForSearch(keys, ls, idxMultis, toAddDimension + 1, offsetIdx + i * idxMultis[toAddDimension])) {
                    return true;
                }
            }
            return false;
        } else {
            return addIdxAndJudgeForSearch(keys, ls, idxMultis, toAddDimension + 1, offsetIdx + ls[toAddDimension]);
        }
    }

    void bloom_filter_compute_indices(long hash, long table_size, long[] bit_index)
    {
        //hash原本是无符整型，但java不支持无符整型，因此使用与运算
        bit_index[0] = ((hash & 0x7fffffffffffffffL) % table_size);
    }
    long bloom_filter_hash_ap(byte[] begin, int remaining_length, long hash)
    {
        int i = 0;
        while(remaining_length >= 2) {
            //hash原本是无符整型，但java不支持无符整型，因此使用无符号右移
            hash ^=    (hash <<  7) ^  (begin[i++]) * (hash >>> 3);
            hash ^= (~((hash << 11) + ((begin[i++]) ^ (hash >>> 5))));
            remaining_length -= 2;
        }
        if (0 != remaining_length) {
            hash ^= (hash <<  7) ^ (begin[i]) * (hash >>> 3);
        }
        return hash;
    }
    void bloom_filter_find_optimal_parameters()
    {
        for (int i = 0; i < dimension; i++) {
            table_sizes_[i] = (long)(-predicted_element_counts_[i] * Math.log(desired_false_positive_probabilitys_[i]) / (Math.log(2.0D) * Math.log(2.0D)));
            salt_counts_[i] = (int)Math.round(table_sizes_[i] / predicted_element_counts_[i] * Math.log(2.0D));

            //开辟每维的特殊位
            table_sizes_[i] += 1;
            //如果table_size 不是64（long元素位数）  的整数倍，补齐
            table_sizes_[i] += (((table_sizes_[i] % BITS_PER_LONG) != 0) ? (BITS_PER_LONG - (table_sizes_[i] % BITS_PER_LONG)) : 0);
        }
    }

    void bloom_filter_generate_unique_salt()
    {
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

        if (salt_.length <= PREDEF_SALT_COUNT) {
            for (int j = 0; j < salt_.length; j++) {
                salt_[j] = predef_salt[j];
            }
            for(int i = 0; i < salt_.length; ++i)
            {
			/*
			  Note:
			  This is done to integrate the user defined random seed,
			  so as to allow for the generation of unique bloom filter
			  instances.
			*/
                salt_[i] = salt_[i] * salt_[(i + 3) % salt_.length];// + random_seed_
            }
        }
        else
        {
            int i = 0;
            int j = 0;
            for (int j2 = 0; j2 < PREDEF_SALT_COUNT; j2++) {
                salt_[j2] = predef_salt[j2];
            }
            Random random = new Random();//random_seed_
            while(i < PREDEF_SALT_COUNT) {
                long current_salt = random.nextLong() * random.nextLong();
                if (0 == current_salt) {
                    continue;
                }
                boolean found = false;
                for(j = 0; j < i; j++) {
                    if(current_salt == salt_[j]) {
                        found = true;
                        break;
                    }
                }
                if(!found) {
                    salt_[i] = current_salt;
                    i++;
                }
            }
        }
    }



}

