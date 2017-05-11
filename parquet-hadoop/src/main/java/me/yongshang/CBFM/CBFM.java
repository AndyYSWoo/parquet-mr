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
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;


public class CBFM{
	// CONFIGs
	// conf 1: infer through fpp & element count
	private long predicted_element_count_ = -1;        		// 期望元素个数
	public static double desired_false_positive_probability_ = -1;	// 期望错误率
	// conf 2: set directly
	public long table_size_ = -1;							// 每个维度上位数组大小[bit], m/64
	// extra conf: add size limit
	public double sizeLimit = -1;							// 空间限制
	public static int reduceStep = 40;								// m调整步长

	public int salt_count_ = 6;                    			// hash函数个数, k, 默认为6

	public static void setIndexedDimensions(String[] columns){
		indexedColumns = columns;
		dimension = indexedColumns.length;
	}
	public static int[] reducedimensions = new int[]{};	// 所删减的维度组合（一个元素表示一个删减组合，使用int的最低的几个位，置为1表示需要砍掉）
	public static int dimension;						// 维度数
	public static String[] indexedColumns;				// 建立索引的列名

	public static boolean ON = false;
	public static boolean DEBUG = true;
	
	// Bit Long conversion
	private static final long[] bit_mask = {
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
	private static final long BITS_PER_LONG = 64;
	// Seeds Generation
	private static final int PREDEF_SALT_COUNT = 128;	//原始种子数量（用于计算每个哈希采用的种子）
	private long[] salt_ = null;						//每个哈希采用的种子（使用一个哈希函数，传入不同的种子，实现多个哈希函数）
	// 1D table
	private long totalBitSize = 1l;	// 总位数组大小[bit]
	private long longLen;			// 总位数组大小[long]
	private long[] bit_table_;		// 位数组，一维long
	
	private static void debugPrint(String str){
		if(CBFM.DEBUG) System.out.println("[CBFM]\t"+str);
	}
	private static void debugPrintEle(String msg,byte[][] bytes){
//		if(DEBUG){
//			System.out.print("[CBFM]\t"+msg+" calc idx for element: ");
//			for (byte[] key : bytes) {
//				System.out.print(Arrays.toString(key)+", ");
//			}
//			System.out.println();
//		}
	}
	private static void debugPrintIdx(String msg,List<Long> idxes){
//		if(CBFM.DEBUG) System.out.println("[CBFM]\t"+msg+" insert indexes: "+idxes);
	}
	public CBFM(long predicted_element_count_){
		this.predicted_element_count_ = predicted_element_count_;
		initParams();
		bloom_filter_generate_unique_salt();
		generateTable();
	}

	private void displayParams(){
		debugPrint("count: "+predicted_element_count_);
		debugPrint("k: "+salt_count_);
		debugPrint("m: "+table_size_);
		debugPrint("size: "+(longLen * 64.0 / (1024*1024))/8.0+"MB");
	}

	/**
	 * Initialize with stored table
	 * @param table
	 */
	public CBFM(long[] table){
		this.bit_table_ = table;
	}

	public CBFM(String compressedString){
		initFromString(compressedString);
		initParams();
		bloom_filter_generate_unique_salt();
	}

	private void initFromString(String str){
		String[] tokens = str.split(",");
		predicted_element_count_ = Long.valueOf(tokens[0]);
		longLen = Integer.valueOf(tokens[1]);
		bit_table_ = new long[(int)longLen];
		for(int i = 0; i<tokens.length-2; ++i){
			int separateIndex = tokens[i+2].indexOf(":");
			bit_table_[Integer.valueOf(tokens[i+2].substring(0,separateIndex))] =
					Long.valueOf(tokens[i+2].substring(separateIndex+1));
		}
	}

	public CBFM(long predicted_element_count, double desired_false_positive_probability, int dimension, int[] reducedimensions)
	{
		predicted_element_count_ = predicted_element_count;
		desired_false_positive_probability_ = desired_false_positive_probability;
		CBFM.dimension = dimension;
		CBFM.reducedimensions = reducedimensions;
		
		table_size_ = 64 * 181l;
		salt_count_ = 6;
		salt_ = new long [salt_count_];
		
		//选种子
		bloom_filter_generate_unique_salt();

		generateTable();
	}

	private void initParams(){
		if(table_size_ == -1){
			// If m is not set, infer it;
			if(desired_false_positive_probability_ != -1 && predicted_element_count_ != -1){
				double f = desired_false_positive_probability_;
				long n = predicted_element_count_;
				int k = (int) Math.floor(-Math.log(f) / Math.log(2)); // k = -log2(f)
				salt_count_ = k;
				int m = (int) Math.ceil(n * (1/Math.log(2)) * (Math.log(1/f)/Math.log(2)));
				m += 1;
				m += (((m % BITS_PER_LONG) != 0) ? (BITS_PER_LONG - (table_size_ % BITS_PER_LONG)) : 0);
				table_size_ = m;
			}else{
				throw new RuntimeException("CBFM CONFIGs are incomplete.");
			}
		}// Otherwise m's given value.
		
		// init salt talbe
		salt_ = new long[salt_count_];
	}
	
	private void generateTable(){
		calcTableSize();
		displayParams();
		bit_table_ = new long[(int)longLen];
	}
	private void calcTableSize(){
		//计算m^d
		for (int i = 0; i < dimension; i++) {
			totalBitSize *= table_size_;
		}
		//删减不使用的位（计算过程，类似于因式分解：(m^r1 - (m-1)^r1)(m^r2 - (m-1)^r2) = m^(r1+r2) - m^r2*(m-1)^r1 - m^r1*(m-1)^r2 + (m-1)^(r1+r2)
		//对于删减维度组之间有共同维度时，保证每个分解项总共是d个m或m-1相乘，（共同删减的维度看作合并）
		// i: 组合元素个数
		// j: 起始元素下标
		for (int i = 0; i < reducedimensions.length; i++) {
			for (int j = 0; j < reducedimensions.length - i; j++) {
				//排除第一个分解项（完整位大小），将其它分解项累加到实际位长度
				//由于因式分解的过程为组合每个因式中的项，代码实现组合的方式为：带有递归的遍历（调用时，指定首选的几个元素及还需要选的元素个数，方法内部会在指定元素后序的元素中遍历、再在选一个元素并递归调用）
				//参数传入处理的维度数、删减的维度组合、已经抽取用于计算分解项的删减维度组序号、还需要选取用于计算分解项的删减维度组个数
				totalBitSize += factorizationModifyNum(dimension, reducedimensions, new int[]{j}, i);
			}
		}
		longLen = ((totalBitSize + BITS_PER_LONG - 1) / BITS_PER_LONG);
		// TODO better way?
		if(sizeLimit != -1){// if size limit is set
			if(longLen*BITS_PER_LONG/(1024*1024.0) > sizeLimit){// execeeds size limit
				debugPrint("table size "+longLen*BITS_PER_LONG/(1024*1024.0)+"M execeeds limit "+sizeLimit+"M, reducing...");
				totalBitSize = 1;
				table_size_-= reduceStep;
				if(table_size_ <= 0) throw new RuntimeException("m <= 0, reducing too fast.");
				calcTableSize();
			}
		}
	}
	
	//由于因式分解的过程为组合每个因式中的项，代码实现组合的方式为：带有递归的遍历（调用时，指定首选的几个元素及还需要选的元素个数，方法内部会在指定元素后序的元素中遍历、再在选一个元素并递归调用）
	/**
	 * 
	 * @param dimension
	 * @param reducedimensions	
	 * @param curidxs			已有元素
	 * @param otherFactorNum	组合元素个数-1
	 * @return
	 */
	private long factorizationModifyNum(int dimension, int[] reducedimensions, int[] curidxs, int otherFactorNum) {
		long normalRet = 0l;
		if (0 == otherFactorNum) {
			//已经选取了全部需要组合的元素
			int combineCutD = 0;
			for (int i : curidxs) {
				if (0 == reducedimensions[i]) {
					return 0l;	//若传入选取的维度组合中，存在不删减维度的组合（对元素计算选取位时会发生），直接不处理
				}
				//将选取的删减维度的组合合并（共有的删减维度，会合并处理）
				combineCutD |= reducedimensions[i];
			}
			//计算分解项的正负号
			long modifyBitNum = 1l;
			if (1 == (curidxs.length % 2)) {
				modifyBitNum = -1l;
			}
			//遍历合并后的删减维度的组合，中的每一位，量删减位就乘m-1，不是就乘m
			for (int i = 0; i < dimension; i++) {
				boolean isCutBit = (combineCutD & 0x1) != 0;
				combineCutD >>>= 1;
				if (isCutBit) {
					modifyBitNum *= (table_size_ - 1);
				} else {
					modifyBitNum *= table_size_;
				}
			}
			normalRet = modifyBitNum;
		} else {
			//需要继续选取一个元素，并递归
			//从已经选取的元素后序中选取
			for (int j = curidxs[curidxs.length - 1] + 1; j < reducedimensions.length - (otherFactorNum - 1); j++) {
				//由于数组不能变长，再new一个
				int [] nextidxs = new int[curidxs.length + 1];
				System.arraycopy(curidxs, 0, nextidxs, 0, curidxs.length);
				//新选取元素
				nextidxs[curidxs.length] = j;
				//递归
				normalRet += factorizationModifyNum(dimension, reducedimensions, nextidxs, otherFactorNum - 1);
			}
		}
		return normalRet;
	}
	
	//计算插入元素对应需要置的位：位的个数 = 维度之间的组合个数 * k
	public ArrayList<Long> calculateIdxsForInsert(byte[][] keys) {
		debugPrintEle("INSERT", keys);
		ArrayList<Long> ret = new ArrayList<Long>();
		//计算每个字段在自己所在维度上的位序号
		long[][] totalIdx = new long[salt_count_][dimension];
		int curD = 0;	//当前计算的维度序号
		for (byte[] key : keys) {
			for(int i = 0; i < salt_count_; ++i) 
			{
				totalIdx[i][curD] = bloom_filter_compute_indices(bloom_filter_hash_ap(key, salt_[i]), table_size_);
			}
			curD++;
		}
		//遍历每个哈希运算
		for (long[] ls : totalIdx) {
			//又是一处使用带递归的遍历，实现组合
			for (int i = 0; i < dimension; i++) {
				for (int j = 0; j < dimension - i; j++) {
					calculateCombine(ret, ls, new int[]{j}, i);
				}
			}
		}
		return ret;
	}

	//计算插入元素对应需要置的位：位的个数 与查询字段与是否有删减维度组合有关（若包含删减维度组合，则拆解组合，递归）
	public ArrayList<Long> calculateIdxsForSearch(byte[][] keys) {
		debugPrintEle("SEARCH",keys);
		ArrayList<Long> ret = new ArrayList<Long>();
		//计算每个字段在自己所在维度上的位序号
		long[][] totalIdx = new long[salt_count_][dimension];
		int curD = 0;	//当前计算的维度序号
		int searchDnum = 0;	//不为null的字段个数
		for (byte[] key : keys) {
			if (null == key) {
				//查询时，可以某些字段没有内容，跳过
				curD++;
				continue;
			}
			searchDnum++;
			for(int i = 0; i < salt_count_; ++i) 
			{
				totalIdx[i][curD] = bloom_filter_compute_indices(bloom_filter_hash_ap(key, salt_[i]), table_size_);
			}
			curD++;
		}
		//获得有元素的字段序号
		int[] searchDcombine = new int[searchDnum];
		int idxOffset = 0;	//跳过为null字段的个数
		for (int i = 0; i < searchDcombine.length; i++) {
			while (null == keys[i + idxOffset]) {
				idxOffset++;
			}
			searchDcombine[i] = i + idxOffset;
		}
		//遍历每个哈希运算
		for (long[] ls : totalIdx) {
			calculateCombineForSearch(ret, ls, searchDcombine, 0);
		}
		debugPrintIdx("SEARCH", ret);
		return ret;
	}
	
	private void calculateCombineForSearch(ArrayList<Long> ret, long[] oneHashIdx, int[] curDimensionIdxs, int otherDimensionNum) {
		//当前要查询的字段组合，表示为位形式：不为null的字段对应位置1
		int curDimensionCombine = 0x0;
		for (int i : curDimensionIdxs) {
			curDimensionCombine |= 0x1 << (dimension - 1 - i);
		}
		for (int i : reducedimensions) {
			if (i == (i & curDimensionCombine)) {
				//若查询的字段组合中，包含删减维度组合
				int cutD = i;
				int checkingD = dimension - 1;
				for (int j = 0; j < dimension; j++) {
					if (0 != (cutD & 0x1)) {
						//处理删减维度组合中的每个删减位：从查询字段组合中排除此位，并递归
						// 删除一个维度后，递归
						int[] newDimensionIdxs = new int[curDimensionIdxs.length - 1];
						int setingD = 0;
						for (int k = 0; k < curDimensionIdxs.length; k++) {
							if (checkingD != curDimensionIdxs[k]) {
								newDimensionIdxs[setingD] = curDimensionIdxs[k];
								setingD++;
							}
						}
						calculateCombineForSearch(ret, oneHashIdx, newDimensionIdxs, otherDimensionNum);
					}
					cutD >>>= 1;
					checkingD--;
				}
				return;
			}
		}
		//查询的字段组合中，不包含删减维度组合，可按插入方式计算
		calculateCombine(ret, oneHashIdx, curDimensionIdxs, otherDimensionNum);
	}
	/**
	 * 
	 * @param ret				返回下标列表
	 * @param oneHashIdx		当前hash函数对所有维度计算结果
	 * @param curDimensionIdxs	当前选取的维度
	 * @param otherDimensionNum	组合元素个数-1
	 */
	private void calculateCombine(ArrayList<Long> ret, long[] oneHashIdx, int[] curDimensionIdxs, int otherDimensionNum) {
		if (0 == otherDimensionNum) {
			//若已经选取全部组合元素
			//当前要查询的字段组合，表示为位形式：不为null的字段对应位置1
			int curDimensionCombine = 0x0;
			for (int i : curDimensionIdxs) {
				// 当前处理的维度组合, curDimensionIdxs从左向右记维度
				curDimensionCombine |= 0x1 << (dimension - 1 - i);
			}
			for (int i : reducedimensions) {
				if (i == (i & curDimensionCombine)) {
					return;	//若查询的字段组合中，包含删减维度组合，直接跳过
				}
			}
			long bitIdxToAdd = 0l;	//需要加入 到返回结果中的位序号
			//计算过程由高维度到低维度遍历，处理每个维度时，因高维度是否有字段内容，产生不同的相对删减维度组合信息，在此定义此相对删减维度组合信息（转换删减维度组合信息）
			int[] convertReduceDimensions = new int[reducedimensions.length];
			//初始化为原始删减维度组合信息
			System.arraycopy(reducedimensions, 0, convertReduceDimensions, 0, reducedimensions.length);
			//由高维度到低维度遍历
			for (int i = curDimensionIdxs.length - 1; i > -1; i--) {
				// 当前处理的维度加更高维度的个数
				int exceptDNum = dimension - curDimensionIdxs[i];	//处理维度及更高维度的个数
				int[] subReduceDimensions = new int[convertReduceDimensions.length];	//此维度中特殊位（首个逻辑位）中子维度的相对删减维度组合信息
				int[] jumpSubReduceDimensions = new int[convertReduceDimensions.length];	//此维度中，除特殊位中，子维度的相对删减维度组合信息
				// 对每个删减组合
				for (int j = 0; j < convertReduceDimensions.length; j++) {//遍历相对删减维度组合信息的删减维度组合
					// 如果有更高维在删减位中
					if (0 != (convertReduceDimensions[j] & ~(0xffffffff << (exceptDNum - 1)))) {//若删减维度组合中删减的位中，包含比处理维度更高的维度
						// 置0
						subReduceDimensions[j] = 0;	
						// 置0
						jumpSubReduceDimensions[j] = 0;
					// 如果当前维在删减位中
					} else if (0 != (convertReduceDimensions[j] & (0x1 << (exceptDNum - 1)))) {//若删减维度组合中删减的位中，包含处理维度
						// 置0
						subReduceDimensions[j] = 0;
						// 
						jumpSubReduceDimensions[j] = convertReduceDimensions[j] >>> exceptDNum;
						// 修改删减位, 去掉当前维
						convertReduceDimensions[j] &= ~(0x1 << (exceptDNum - 1));
					// 当前维与更高维均不在删减位中
					} else {//删减维度组合中删减的位中，不包含比处理维度更高的维度及处理维度
						// 
						subReduceDimensions[j] = convertReduceDimensions[j] >>> exceptDNum;
						// 
						jumpSubReduceDimensions[j] = convertReduceDimensions[j] >>> exceptDNum;
					}
				}
				//计算此维度的维度系数
				long bitIdxMulti = 1l;
				for (int j = 0; j < curDimensionIdxs[i]; j++) {
					bitIdxMulti *= table_size_;
				}
				//计算不砍维度下的位序号
				bitIdxToAdd += bitIdxMulti * oneHashIdx[curDimensionIdxs[i]];
				
				//删除此维度中特殊位（首个逻辑位）中的砍掉位个数
				for (int i1 = 0; i1 < subReduceDimensions.length; i1++) {
					for (int j = 0; j < subReduceDimensions.length - i1; j++) {
						bitIdxToAdd += factorizationModifyNum(curDimensionIdxs[i], subReduceDimensions, new int[]{j}, i1);
					}
				}
				
				if (1 < oneHashIdx[curDimensionIdxs[i]]) {
					//若此维度上的逻辑位大于1
					//删除此维度中，跳过的非特殊位中的砍掉位个数
					for (int i1 = 0; i1 < jumpSubReduceDimensions.length; i1++) {
						for (int j = 0; j < jumpSubReduceDimensions.length - i1; j++) {
							bitIdxToAdd += (oneHashIdx[curDimensionIdxs[i]] - 1) * factorizationModifyNum(curDimensionIdxs[i], jumpSubReduceDimensions, new int[]{j}, i1);
						}
					}
				}
			}
			ret.add(bitIdxToAdd);
		} else {
			//需要继续选取一个元素，并递归
			//从已经选取的元素后序中选取
			for (int j = curDimensionIdxs[curDimensionIdxs.length - 1] + 1; j < dimension - (otherDimensionNum - 1); j++) {
				//由于数组不能变长，再new一个
				int [] nextDimensionIdxs = new int[curDimensionIdxs.length + 1];
				System.arraycopy(curDimensionIdxs, 0, nextDimensionIdxs, 0, curDimensionIdxs.length);
				//新选取元素
				nextDimensionIdxs[curDimensionIdxs.length] = j;
				//递归
				calculateCombine(ret, oneHashIdx, nextDimensionIdxs, otherDimensionNum - 1);
			}
		}
	}

	//插入
	public void insert(ArrayList<Long> bitIdxs)
	{
		for (Long long1 : bitIdxs) {
		    bit_table_[(int)(long1 / BITS_PER_LONG)] |= bit_mask[(int)(long1 % BITS_PER_LONG)];
		}
	}

	//判断是否可能存在
	public boolean contains(ArrayList<Long> bitIdxs)
	{
		for (Long long1 : bitIdxs) {
			if (0 == long1) {
				continue;	//容错处理，查询时，不应该执行到这里
			}
			if (0 == (bit_table_[(int) (long1 / BITS_PER_LONG)] & bit_mask[(int) (long1 % BITS_PER_LONG)])) {
				return false;
			}
		}
		return true;
	}
	
	//对哈希值取m-1的模，同时 +1(跳过特殊位)
	// 所获得的下标范围是[1,m)
	long bloom_filter_compute_indices(long hash, long table_size)
	{
		//hash原本是无符整型（c语言版本中），但java不支持无符整型，因此使用与运算
		return ((hash & 0x7fffffffffffffffL) % (table_size - 1)) + 1;
	}
	
	//哈希函数（不了解原理，这部分与矩阵、砍维度没关系）
	long bloom_filter_hash_ap(byte[] begin, long hash)
	{
		int remaining_length = begin.length;
		int i = 0;
		while(remaining_length >= 2) {
			hash ^=    (hash <<  7) ^  (begin[i++]) * (hash >>> 3);
			hash ^= (~((hash << 11) + ((begin[i++]) ^ (hash >>> 5))));
			remaining_length -= 2;
		}
		if (0 != remaining_length) {
			hash ^= (hash <<  7) ^ (begin[i]) * (hash >>> 3);
		}
		return hash;
	}
	

	//计算每个哈希的种子，为什么这么算，我也不知道（这部分与矩阵、砍维度没关系）
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
	    
		if (salt_count_ <= PREDEF_SALT_COUNT) {
			for (int j = 0; j < salt_count_; j++) {
				salt_[j] = predef_salt[j];
			}
			for(int i = 0; i < salt_count_; ++i) 
			{
				/*
				  Note:
				  This is done to integrate the user defined random seed,
				  so as to allow for the generation of unique bloom filter
				  instances.
				*/
				salt_[i] = salt_[i] * salt_[(i + 3) % salt_count_];
			}
		} 
		else 
		{
			int i = 0;
	        int j = 0;
	        for (int j2 = 0; j2 < PREDEF_SALT_COUNT; j2++) {
	        	salt_[j2] = predef_salt[j2];
			}
	        Random random = new Random();
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

	public long[] getTable(){
		return bit_table_;
	}

	public String compressTable(){
		return compressFromTable(this.predicted_element_count_, this.bit_table_);
	}

	/**
	 * 将long数组转化为字符串表示, 格式为 len,[index:value]+
	 * @param table
	 * @return
     */
	public static String compressFromTable(long predicted_element_count_, long[] table){
		if(table == null) return null;
		StringBuilder sb = new StringBuilder();
		sb.append(predicted_element_count_+",");
		sb.append(table.length+",");
		for (int i = 0; i < table.length; i++) {
			if(table[i] != 0) sb.append(i+":"+table[i]+",");
		}
		String compressedStr = sb.toString();
//		debugPrint("compressed table: "+compressedStr);
		return compressedStr;
	}

	public void serialize(DataOutput out) throws IOException {
		long[] table = this.bit_table_;
		for (int i = 0; i < table.length; i++) {
			if(table[i] != 0){
				out.write(ByteBuffer.allocate(8).putLong(table[i]).array());
			}
		}
	}
}
