package org.compHuff;

import java.io.ByteArrayOutputStream;
import java.lang.Thread.State;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.EntryWeigher;

/**
 * CompressHuffman is a library to compress isolated records of data in a DB using a shared huffman tree generated from all the records.
 * This library was created because compressing a single record in a DB using DEFLATE or other methods produces poor compression (5-10%) due to
 * high entropy of individual records (due to small size). Compress huffman exploits the low entropy of the entire dataset to produce a huffman tree 
 * that can compress most individual records with 30-70% compression (depending on data). 
 * Using the VM option -XX:+UseCompressedOops can speed things up by about 10% as long as your heap is <32GB
 * 
 * This library is free to use and is under the apache 2.0 licence, available @ https://www.apache.org/licenses/LICENSE-2.0
 *
 * Useage: feed you'r dataset (byte[record][recordData] for in memory datset or Iterable<byte[recordData]> for retreival from DB) to the Constructor
 * You can then use compress(byte[]) decompress(byte[])  after the HuffmanTree has been generated.
 * 
 * To Store the HuffmanTree for future use without having the generate it again use getHuffData() and store it in a file
 * Read and feed the byte array to new CompressHuffman(byte[]) at a future data then use can use  compress(byte[]) decompress(byte[]) 
 * 
 * For more info on how the library works and to report bugs visit  https://github.com/MPdaedalus/CompressHuffman
 * 
 * @author daedalus 
 * @version 1.0
 */

public class CompressHuffman {
	public int nodeId, symbl2CodLstIdx, maxSymbolLength, defsymbl2CodLstIdx, altCodeIdx, altCodeBytes, removedBytes, highestLevel, totalSymbols;
	public byte[][] codeValues, symbol2Code, defsymbol2Code, defCodeValues,tmpsymbol2Code,tmpCodeValues;
	public byte[][][] codeIdx2Symbols, defCodeIdx2Symbols, tmpCodeIdx2Symbols;
	boolean useAltOnly;
	HuffConfig config;
	String symbolFile;
	PriorityQueue<Weight > trees;
	EntryWeigher<ByteAry, Integer> memoryUsageWeigher = new EntryWeigher<ByteAry, Integer>() {
		@Override
		public int weightOf(ByteAry key, Integer value) {
			return key.ary.length + 32;
		}
	};
	ConcurrentMap<ByteAry, Integer>  freqList;
	public long eightyPCMemory;
	public static String[] charsByFreq = new String[]{" ","e","t","a","o","i","n","s","r","h","l","d","c","u","m","f","p","g","w","y","b","v","k","x","j","q","z","E","T","A","O","I","N","S","R","H","L","D","C","U","M","F","P","G","W","Y","B","V","K","X","J","Q","Z",",",".","0","1","2","3","4","5","6","7","8","9","'","\"",";",")","(",":","!","?","/","&","-","%","@","$","_","\\","*","=","[","]","+",">","<","^","`","|","~","{","}","¢","£","©","®","°","±","²","³","µ","¼","½","¾","÷"};
	//hash stuff
    static final long PRIME64_1 = -7046029288634856825L; //11400714785074694791
    static final long PRIME64_2 = -4417276706812531889L; //14029467366897019727
    static final long PRIME64_3 = 1609587929392839161L;
    static final long PRIME64_4 = -8796714831421723037L; //9650029242287828579
    static final long PRIME64_5 = 2870177450012600261L;
	
	private int aryHash(byte[] ary) {
		if(ary.length == 0 ) return 1;
		return longHash(hash(ary, 0, ary.length, -1640531527));
	}

	class ByteAry {
		public byte[] ary;
		public ByteAry(byte[] ary) {
			this.ary = ary;
		}
		@Override
		public int hashCode() {
			return longHash(hash(ary, 0, ary.length, -1640531527));
		}
		@Override
		public boolean equals(Object obj) {
			return Arrays.equals(ary, ((ByteAry) obj).ary);
		}
	}

	interface Weight {
		public long getWeight();
	}

	class TmpNode implements Comparable<Weight>, Weight {

		public long wt;
		public byte[] key;
		@Override
		public int compareTo(Weight o) {
			int c =  Long.compare(wt, o.getWeight());
			if(c == 0)return 1;  
			return c;
		}
		@Override
		public int hashCode() {
			return longHash(hash(key, 0, key.length, -1640531527));
//			int hash = 1;
//			for(byte b : key) hash = (257 * hash + b);
//			return hash;
		};

		public TmpNode(long weight, byte[] key) {
			this.wt = weight;
			this.key = key;
		}
		@Override
		public long getWeight() {
			return wt;
		}
	}
/**
 *  Use max symbolLength of 8, max symbols 10000
 *  Est. Decompression speed 50-55MB/Sec per core , Est. Compression 45%
 *  This Setting also produces the lowest memory usage for the Compressor/Decompressor as well as shortest huffTree build time
 *  Est. HuffTree build speed=3MB/Sec (of dataSet) on quad core CPU, ,  Est. Final HuffData size=100KB-1MB
 */
	public static HuffConfig fastestCompDecompTime() {
		return config(8,10000,2000000000,false,20);
	}
	/**
	 *  Use max symbolLength of 10, max symbols 1 million
	 *  Est. Decompression speed 30-35MB/Sec per core , Est. Compression 60%
	 *  This Setting also produces the highest memory usage and longest comp/decompress time for the Compressor/Decompressor 
	 *  as well as longest huffTree build time
	 *  Est. HuffTree build speed=2MB/Sec (of dataSet) on quad core CPU,  Est. Final HuffData size=10-20MB
	 */
	public static HuffConfig smallestFileSize() {
		return config(10,1000000,2000000000,false,26);
	}
	
	/**
	 * With maxSymbolLength and maxSymbols higher values may give better compression at expense of longer compress/decompress time
	 * and much longer (many times) hufftree generation time.
	 * 
	 * @param maxSymbolLength 
	 * default=9,  Diminishing returns beyond 8.
	 * @param maxSymbols 
	 * default=100000, Diminishing returns beyond 1 million 
	 * @param maxSymbolCacheSize 
	 * default=2GB, will use half of free memory if less than 2GB available. Bigger symbol cache does not always improve compression beyond 2GB
	 * @param twoPass 
	 * default=false, whether to generate dummy hufftree and do dummy compress to eliminate unused symbols, improves compression by a few % 
	 * but hufftree generation takes twice as long or more, has little effect to comp/decomp time
	 * @param maxTreeDepth 
	 * default=20, shorter treeDepth reduces memory needed for huffman Tree but will reduce compression 
	 * Do not go below indexOf(highest set bit(2 x number of records)) or above 31, or you may get error.
	 */
	public static HuffConfig config(int maxSymbolLength, int maxSymbols, long maxSymbolCacheSize, boolean twoPass, int maxTreeDepth) {
		HuffConfig hc = new HuffConfig();
		hc.maxSymbolLength = maxSymbolLength;
		hc.maxSymbols = maxSymbols;
		hc.maxSymbolCacheSize = maxSymbolCacheSize;
		hc.twoPass = twoPass;
		hc.maxTreeDepth = maxTreeDepth;
		return hc;
	}
	
	static class HuffConfig {
		int maxSymbolLength, maxSymbols,maxTreeDepth;
		long maxSymbolCacheSize;
		boolean twoPass;
	}
	//Note: allocating big dataBuffer rather then checking and resizing buffer is faster as long as buffer limit not reached (will cause outofbounds  error)
	public byte[] deCompress(byte[] codes) {
		if(codes.length == 0) return new byte[0];
		byte[] data = new byte[codes.length*20];
		byte[] finalData;
		int dataIdx = 0;
		byte unCompSymb = 0;
		int codeIdx=3;
		byte[] symbol;
		int symbolLen;
		int symbolIdx = 0;
		final int codesLen = (codes.length*8)-(byte) (codes[0] & 0b111);
		int bitLen;
		while(codeIdx<codesLen) {
			symbolIdx = 0;
			symbol = null;
			bitLen = 0;
			//add codes to symbolIdx bit by bit until symbol is found, &7 == % 8, this loop takes up the majority of the total decompress time
			while(symbol == null && codeIdx<codesLen) {
				symbolIdx |= ((byte)((codes[codeIdx/8]) >>> (codeIdx++&7)) & (byte)1) << bitLen;
				symbol = codeIdx2Symbols[++bitLen][symbolIdx];
			}
			if(symbol == null) break;
			symbolLen = symbol.length;
			if(symbolLen == 0) {
				if(((codes[codeIdx/8] >>(codeIdx&7))&1)==1) {
					unCompSymb = 0;
					for(int i=0; i<8; i++) {
						codeIdx++;
						if(((codes[codeIdx/8] >>(codeIdx&7))&1)==1) unCompSymb |= 1 << i;
					}
					//if(dataIdx+1 >=data.length) data = expand(data,1);
					data[dataIdx++] = unCompSymb; 
					codeIdx++;
				} else {
					codeIdx++;
					symbolIdx = 0;
					bitLen = 1;
					//add codes to symbolIdx bit by bit until symbol is found 
					while((symbol = defCodeIdx2Symbols[bitLen++][symbolIdx = (symbolIdx << 1) | ((byte)((codes[codeIdx/8]) >>> (codeIdx++&7)) & (byte)1)]) == null && codeIdx<codesLen);
					if(symbol == null) {
						while((symbol = defCodeIdx2Symbols[bitLen++][symbolIdx = (symbolIdx << 1) | 0]) == null);
						symbolLen = symbol.length;
						//if(dataIdx + symbolLen >= data.length) data = expand(data,symbolLen);
						System.arraycopy(symbol , 0, data, dataIdx,  symbolLen);
						dataIdx +=  symbolLen;
						continue;
					}
					symbolLen = symbol.length;
					//if(dataIdx + symbolLen >= data.length) data = expand(data,symbolLen);
					System.arraycopy(symbol , 0, data, dataIdx,  symbolLen);
					dataIdx +=  symbolLen;
				}
			} else {
				System.arraycopy(symbol , 0, data, dataIdx,  symbolLen);
				dataIdx +=  symbolLen ;
			}
		}
		if(dataIdx != data.length) {
			finalData = Arrays.copyOfRange(data, 0, dataIdx);
			return finalData;
		}
		return data;
	}

	public byte[] compress(byte[] data) {
		if(data.length == 0) return new byte[0];
		byte[] codes = new byte[data.length];
		int codesIdx = 3;
		int startIdx = 0;
		int endIdx = maxSymbolLength;
		int dataLen = data.length;
		byte[] aCode;
		int curMatchIdx = -1;
		while(true) {
			if(endIdx > dataLen) endIdx = dataLen;
			curMatchIdx =  findSymblIdx(startIdx,endIdx,data,symbl2CodLstIdx, symbol2Code);
			if(curMatchIdx == -1) curMatchIdx = altCodeIdx;
			aCode = codeValues[curMatchIdx];
			if(curMatchIdx != altCodeIdx && (aCode[0]+7)/8 > symbol2Code[curMatchIdx].length+altCodeBytes ) {
				aCode = codeValues[altCodeIdx];
				curMatchIdx = altCodeIdx;
			}
			if(((codesIdx+7)/8)+((aCode[0]+7)/8) > codes.length) codes = expand(codes,(aCode[0]+7)/8);
			codes = addSymbolToCodes(codes,codesIdx,aCode);
			codesIdx += aCode[0];
			//if symbol not in main tree consult default tree, if not in default tree then just add uncompressed
			if(curMatchIdx == altCodeIdx) {
				if(useAltOnly) {
					if(((codesIdx+16)/8) > codes.length) codes = expand(codes,2);
					//set raw bit to true and add byte without compression
					codes[codesIdx/8] |= 1 << (codesIdx &7);
					codesIdx++;
					for(int q=0; q<8; q++) {
						if(((data[startIdx] >> q) & 1)== 1)  codes[codesIdx/8] |= 1 << (codesIdx &7);
						codesIdx++;
					}
					startIdx++;
				} else {
					curMatchIdx =  findSymblIdx(startIdx,endIdx,data,defsymbl2CodLstIdx, defsymbol2Code);
					if(curMatchIdx != -1) {
						codesIdx++;
						aCode = defCodeValues[curMatchIdx];
						if(((codesIdx+7)/8)+((aCode[0]+7)/8) > codes.length) codes = expand(codes,(aCode[0]+7)/8);
						codes = addSymbolToCodes(codes,codesIdx,aCode);
						codesIdx += aCode[0];
						startIdx+=defsymbol2Code[curMatchIdx].length;
					} else {
						if(((codesIdx+16)/8) > codes.length) codes = expand(codes,2);
						//set raw bit to true and add byte without compression
						codes[codesIdx/8] |= 1 << (codesIdx &7);
						codesIdx++;
						for(int q=0; q<8; q++) {
							if(((data[startIdx] >> q) & 1)== 1)  codes[codesIdx/8] |= 1 << (codesIdx &7);
							codesIdx++;
						}
						startIdx++;
					}
				}
			} else {
				startIdx+=symbol2Code[curMatchIdx].length;
			}
			if(startIdx >= data.length) break;
			endIdx = startIdx+maxSymbolLength;
		}
		int actualBytes = (codesIdx+7) /8;
		codes  = Arrays.copyOf(codes , actualBytes);
		byte leftOverBits = (byte) ((actualBytes *8)-codesIdx) ;
		codes[0] |= leftOverBits;
		return codes;
	}

	private byte[] addSymbolToCodes(byte[] codes, int codesIdx, byte[] aCode) {
		int numCodeBts = aCode[0];
		int bitCount=0;
		int codeByteIdx=1;
		for(int q=0; q<numCodeBts; q++) {
			if(((aCode[codeByteIdx] >> bitCount++) & 1)== 1)  codes[codesIdx/8] |= 1 << (codesIdx &7);
			codesIdx++;
			if(bitCount == 8) {
				bitCount = 0;
				codeByteIdx++;
			}
		}
		return codes;
	}
	// (this method takes up majority of compression time)
	private int findSymblIdx(int startIdx, int endIdx, byte[] data, int symbLastIdx, byte[][] symKeySet) {
		int hash = 1;
		int curMatchIdx = -1;
		int symbolIdx, tmpIdx;
		byte[] aKey;
		for(int i=startIdx; i<endIdx; i++) {
			hash = longHash(hash(data, startIdx, (i-startIdx)+1, -1640531527));
			symbolIdx = hash & symbLastIdx;
			probe: while((aKey =symKeySet[symbolIdx]) != null) {
				tmpIdx = symbolIdx;
				if(++symbolIdx == symKeySet.length) symbolIdx = 0;
				if(aKey.length == (i-startIdx)+1) {
					for(int w=0; w<aKey.length; w++) if(startIdx+w == endIdx || aKey[w] != data[startIdx+w]) continue probe; 
				} else {
					continue;
				}
				curMatchIdx = tmpIdx;
			}
		}
		return curMatchIdx;
	}
	
	class RawData implements Iterable<byte[]> {
		private byte[][] data;
		private int len;

		public RawData(byte[][] data) {
			this.data = data;
			len = data.length;
		}
		class ByteIter implements Iterator<byte[]> {
			private int dataIdx = 0;

			@Override
			public boolean hasNext() {
				return dataIdx < len;
			}
			@Override
			public byte[] next() {
				return data[dataIdx++];
			}
		}
		@Override
		public Iterator<byte[]> iterator() {
			return new ByteIter();
		}
	}
	/**
	 * Construct Huffman Tree using maxSymbol Size of 9, max number of symbols 100000, max Tree depth of 20
	 * These settings are a good tradeoff between comp/decompress speed and compression level
	 * Est. Decompression speed 40-45MB/Sec per core , Est. Compression 55%
	 * Est. HuffTree build speed=2-3MB/Sec (of dataSet) on quad core CPU,  Est. Final HuffData size=1-2MB
	 * @param data
	 *	The data Set
	 */
	public CompressHuffman(byte[][] data) {
		generateHuffData(new RawData(data),config(9, 100000, Runtime.getRuntime().freeMemory()/2 < 2000000000 ? Runtime.getRuntime().freeMemory()/2 : 2000000000, false, 20));
	}
	/**
	 * 
	 * @param data
	 * The data Set
	 * @param hc
	 * Config File (see config() method docs)
	 */
	public CompressHuffman(byte[][] data, HuffConfig hc) {
		generateHuffData(new RawData(data),hc);
	}
	
	/**
	 * Construct Huffman Tree using maxSymbol Size of 9, max number of symbols 100000, max Tree depth of 20
	 * These settings are a good tradeoff between comp/decompress speed and compression level
	 * Est. Decompression speed 40-45MB/Sec per core , Est. Compression 55%
	 * Est. HuffTree build speed=2-3MB/Sec (of dataSet) on quad core CPU, Est. Final HuffData size=1-2MB
	 * @param data
	 *	The data Set
	 */
	public CompressHuffman(Iterable<byte[]> data) {
		generateHuffData(data,config(9, 100000, Runtime.getRuntime().freeMemory()/2 < 2000000000 ? Runtime.getRuntime().freeMemory()/2 : 2000000000, false, 20));
	}

	public void generateHuffData(Iterable<byte[]> data, HuffConfig hc) {
		System.out.println("Compress Huffman: Building hufftree");
		config = hc;
		this.maxSymbolLength = hc.maxSymbolLength > 30 ? 30 : hc.maxSymbolLength;
			freqList = new ConcurrentLinkedHashMap.Builder<ByteAry ,Integer>().maximumWeightedCapacity(config.maxSymbolCacheSize).weigher(memoryUsageWeigher)
					.concurrencyLevel(Runtime.getRuntime().availableProcessors()*8).initialCapacity(config.maxSymbolCacheSize > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) config.maxSymbolCacheSize).build();
			useAltOnly = false;
			if(hc.twoPass) {
				System.out.println("Compress Huffman: compiling symbol freq, Stage one of four");
				generateSymbolFreqs(data,false);
				System.out.println("Compress Huffman: building dummy huffTree, Stage two of four");
				freqToTree(1,false);
				buildHuffTree(false,false);
				freqList.clear();
				System.out.println("Compress Huffman: dummy compress to find unused symbols, Stage three of four");
				generateSymbolFreqs(data,true);
				System.out.println("Compress Huffman: build final huffTree, Stage four of four");
				freqToTree(1,true);
				buildHuffTree(false,true);
				System.out.println("Compress Huffman: Done building HuffTree! Use getHuffData() to store tree");
			} else {
				System.out.println("Compress Huffman: compiling symbol freq, Stage one of two");
				generateSymbolFreqs(data,false);
				System.out.println("Compress Huffman: building huffTree Stage two of two");
				freqToTree(1,true);
				buildHuffTree(false,true);
				System.out.println("Compress Huffman: Done building HuffTree! Use getHuffData() to store tree");
			}
			altCodeBytes = (codeValues[1][0]+7)/8;
			switchFields(true);
			buildAltTree(false);
	}
	/**
	 * 
	 * @param data
	 * The data Set
	 * @param hc
	 * Config File (see config() method docs)
	 */
	public CompressHuffman(Iterable<byte[]> data, HuffConfig hc) {
		generateHuffData(data,hc);
	}
	
	//convert freqList hashmap to priorty queue ordered by frequency (lowest first)
	private void freqToTree(int freqDivide, boolean addAltNode) {
		removedBytes  = 1;
		int maxSymbols = config.maxSymbols;
		Entry<ByteAry,Integer> ent;
		trees = new PriorityQueue<Weight>();
		if(freqDivide < 2) {
			for(Iterator<Entry<ByteAry,Integer>> it = freqList.entrySet().iterator(); it.hasNext();) {
				ent = it.next();
				if(ent.getValue() >3) {
					trees.add(new TmpNode(ent.getValue()*ent.getKey().ary.length , ent.getKey().ary));
				} else {
					removedBytes += ent.getValue()*ent.getKey().ary.length;
				}
			}
			while(trees.size() > maxSymbols) trees.poll();
			if(addAltNode) trees.add(new TmpNode(removedBytes  , new byte[0]));
		} else {
			int freq;
			for(Iterator<Entry<ByteAry,Integer>> it = freqList.entrySet().iterator(); it.hasNext();) {
				ent = it.next();
				if(ent.getValue() >3) {
					freq = (ent.getValue()/freqDivide)*ent.getKey().ary.length;
					if(freq == 0) freq = 1;
					trees.add(new TmpNode(freq  , ent.getKey().ary));
				} else {
					removedBytes += ent.getValue()*ent.getKey().ary.length;
				}
			}
			while(trees.size() > maxSymbols) trees.poll();
			if(addAltNode) trees.add(new TmpNode(removedBytes/freqDivide == 0 ? 1 : removedBytes/freqDivide  , new byte[0]));
		}
	}

	private void buildAltTree(boolean useDefault) {
		if(useDefault || trees.size() == 0) {
			trees = new PriorityQueue<Weight>();
			for(int i=0; i<charsByFreq.length; i++) {
				trees.add(new TmpNode((charsByFreq.length*100)-(i*90), charsByFreq[i].getBytes()));
			}
		} 
		buildHuffTree(true,false);
		switchFields(false);
	}

	private void buildHuffTree(boolean isAlt, boolean isFinal) {
		// remove the two trees with least frequency then put them into a new node and insert into the queue
		Weight w1,w2;
		HuffmanTree hn, hf1, hf2;
		int longTreeId = 0;
		totalSymbols= trees.size();
		System.out.println("total symbols=" + totalSymbols);
		int maxTreeDepth;
		if(isAlt) { maxTreeDepth = 26; } else { maxTreeDepth=config.maxTreeDepth > 31 ? 31 : config.maxTreeDepth; };
		int freqDivide = 1;
		HuffmanTree objectTree;
		while(true) {
			longTreeId = 0;
			nodeId = 0;
			while (trees.size() > 1) {
				w1 = trees.poll();
				if(w1 instanceof TmpNode ) {
					hf1 = new HuffmanLeaf(w1.getWeight(),((TmpNode) w1).key);
				} else {
					hf1 = (HuffmanTree) w1;
				}
				w2 = trees.poll();
				if(w2 instanceof TmpNode ) {
					hf2 = new HuffmanLeaf(w2.getWeight(),((TmpNode) w2).key);
				} else {
					hf2 = (HuffmanTree) w2;
				}
				hn = new HuffmanNode(hf1,hf2);
				trees.add(hn);
				if(longTreeId ==  0 || hf1.id == longTreeId || hf2.id == longTreeId) {
					longTreeId = hn.id;
				}
			}
			objectTree = (HuffmanTree) trees.poll();
			highestLevel=0;
			findMaxDepth(((HuffmanNode) objectTree).left, 1);
			findMaxDepth(((HuffmanNode) objectTree).right, 1);
			if(highestLevel <= maxTreeDepth) break;
			freqDivide*= 2;
			freqToTree(freqDivide,true);
		}
		codeIdx2Symbols = new byte[highestLevel+1][][];
		for(int i=1; i<codeIdx2Symbols.length; i++) codeIdx2Symbols[i] = new byte[1 << i][];
		//make sure size power of 2 for  symbl2CodLstIdx & hash
		symbol2Code = new byte[(totalSymbols*2 & (totalSymbols*2 - 1)) == 0 ? totalSymbols*2 : nextPO2(totalSymbols*2)][];
		codeValues = new byte[symbol2Code.length][];
		symbl2CodLstIdx = symbol2Code.length-1;
		populateLUTNCodes(((HuffmanNode) objectTree).left,new byte[] {1,0});
		populateLUTNCodes(((HuffmanNode) objectTree).right,new byte[] {1,1});
		if(isAlt == false && isFinal == true || freqList == null) {
			int symbolIdx = (aryHash(new byte[0]) & symbl2CodLstIdx)-1;
			if(symbolIdx+1 == symbol2Code.length) symbolIdx = -1; 
			byte[] aKey;
			while((aKey =symbol2Code[++symbolIdx]) != null) {
				if(aKey.length == 0) {
					altCodeIdx = symbolIdx;
					break;
				}
			}
		}
	}

	private void findMaxDepth(HuffmanTree objectTree, int level) {
		if (objectTree instanceof HuffmanNode) {
			findMaxDepth(((HuffmanNode) objectTree).left, level+1);
			findMaxDepth(((HuffmanNode) objectTree).right, level+1);
		} else {
			if(level > highestLevel) highestLevel = level;
		}
	}
	//curCode first byte= num of bits used so far 
	private void populateLUTNCodes(HuffmanTree objectTree,byte[] curCode) {
		if (objectTree instanceof HuffmanNode) {
			byte[] leftCode, rightCode;
			if(++curCode[0] > (curCode.length-1)*8) {
				leftCode = new byte[curCode.length+1]; 
				System.arraycopy(curCode, 0, leftCode, 0, curCode.length);
			} else { 
				leftCode = curCode; 
			}
			curCode = null;
			rightCode = Arrays.copyOf(leftCode, leftCode.length);
			int bitIdx;
			if((rightCode[0] &7) == 0) bitIdx = 7; else bitIdx =(rightCode[0] &7)-1;
			rightCode[rightCode.length-1]  |= 1 << bitIdx;
			populateLUTNCodes(((HuffmanNode) objectTree).left,leftCode);
			populateLUTNCodes(((HuffmanNode) objectTree).right,rightCode);
		} else {
			byte[] symbol = ((HuffmanLeaf) objectTree).val;
			int bitCount=0;
			int codeByteIdx=1;
			int codeV = 0;
			for(int i=0, len=curCode[0]; i<len; i++) {
				if(((curCode[codeByteIdx] >> bitCount++) & 1)== 1)  codeV |= 1 << i;
				if(bitCount == 8) {
					bitCount = 0;
					codeByteIdx++;
				}
			}
			codeIdx2Symbols[curCode[0]][codeV] = symbol;
			int hashIdx = aryHash(symbol) & symbl2CodLstIdx;
			while(symbol2Code[hashIdx] != null) if(++hashIdx == symbol2Code.length) hashIdx = 0;
			symbol2Code[hashIdx] = symbol;
			codeValues[hashIdx] = curCode;
		}
	}
	//generate symbols and record their frequency in dataset
	class SFGenerator implements Runnable {

		ArrayBlockingQueue<byte[]> workQueue;

		public SFGenerator(ArrayBlockingQueue<byte[]> workQueue) {
			this.workQueue = workQueue;
		}
		@Override
		public void run() {
			int VLen;
			Integer weight, oldWeight;
			byte[] symbol;
			byte[] dV;
			ByteAry ba;
			try {
				while(true) {
					dV = workQueue.take();
					VLen = dV.length;
					for(int i=0; i<VLen ; i+=maxSymbolLength) {
						for(int j=i; j<VLen && j<i+maxSymbolLength; j++) {
							symbol  = Arrays.copyOfRange(dV, i, j+1);
							ba = new ByteAry(symbol);
							while(true) {
								oldWeight = freqList.get(ba);
								if(oldWeight != null) {
									weight=oldWeight+symbol.length; 
									if(freqList.replace(ba, oldWeight,weight)) break;
								} else { 
									weight =  symbol.length;
									if(freqList.putIfAbsent(ba, weight) == null) break;
								}
							}
						}
							
					}
				}
			} catch (InterruptedException e) {
				return;
			}
		}
	}

	//find out what symbols are really used during compression and add to freqList
	class FUGenerator implements Runnable  {
		int startIdx, endIdx, symbIdx;
		byte[] symbol;
		ByteAry bAry;
		Integer weight, oldWeight;
		byte[] ba;
		ArrayBlockingQueue<byte[]> workQueue;

		public FUGenerator(ArrayBlockingQueue<byte[]> workQueue) {
			this.workQueue = workQueue;
		}
		@Override
		public void run() {
			try {
				while(true) {
					ba = workQueue.take();
					if(ba.length == 0) continue;
					startIdx = 0;
					endIdx = ba.length < maxSymbolLength ? ba.length : maxSymbolLength;
					while(true) {
						symbIdx = findSymblIdx(startIdx, endIdx, ba, symbl2CodLstIdx, symbol2Code);
						if(symbIdx != -1) {
							//symbUsed[symbIdx] = true;
							symbol = symbol2Code[symbIdx];
							bAry = new ByteAry(symbol);
							while(true) {
								oldWeight = freqList.get(bAry);
								if(oldWeight != null) {
									weight = oldWeight+symbol.length;
									if(freqList.replace(bAry, oldWeight,weight)) break;
								} else {
									weight = symbol.length; 
									if(freqList.putIfAbsent(bAry, weight) == null) break;
								}
							}
							startIdx+=symbol.length;
						} else {
							startIdx+=endIdx-startIdx;
						}
						if(startIdx >= ba.length) break;
						endIdx = startIdx+maxSymbolLength;
						if(endIdx > ba.length) endIdx = ba.length;
					}
				}
			}catch (InterruptedException e) {
				return;
			}
		}
	}
	//setup multithreading
	private void generateSymbolFreqs(Iterable<byte[]>data, boolean isFindUsed) {
		int cores = Runtime.getRuntime().availableProcessors();
		//workQueue = new ArrayBlockingQueue<>(cores*5);
		Thread[] workers = new Thread[cores];
		@SuppressWarnings("unchecked")
		ArrayBlockingQueue<byte[]>[] queues = new ArrayBlockingQueue[cores];
		for(int i=0; i<cores; i++) {
			queues[i] = new ArrayBlockingQueue<>(50);
			if(isFindUsed) {
				workers[i] = new Thread(new FUGenerator(queues[i]), "CompHuffFUGenerator " + i);
			} else {
				workers[i] = new Thread(new SFGenerator(queues[i]), "CompHuffSFGenerator " + i);
			}
			workers[i].start();
		}
		try {
			long n = 0, counter = 0;
			for(byte[] ba : data) {
				if((counter++ & 131071) == 0)  System.out.println("CompressHuffman: Processed Records=" + counter);
				if(ba != null) while(!queues[(int) (n++ % cores)].offer(ba));
			}
			while(true) {
				cores=0;
				for(Thread t : workers) if(t.getState() == State.WAITING) cores++;
				if(cores == workers.length) break;
				Thread.sleep(20);
			}
			System.out.println("CompressHuffman: Finished Stage, Processed Records=" + counter);
			for(Thread t : workers) t.interrupt();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

	private int nextPO2(int v) {
		v--;
		v |= v >> 1;
		v |= v >> 2;
		v |= v >> 4;
		v |= v >> 8;
		v |= v >> 16;
		v++;
		return v;
	}

	public int numBitsForNumber(long x) {
		return 63 - Long.numberOfLeadingZeros(x);
	}

	private byte[] expand (byte[] ary, int nextCodeLen) {
		byte[] newAry = new byte[(ary.length*2)+nextCodeLen];
		System.arraycopy(ary, 0, newAry, 0, ary.length);
		return newAry;
	}

	abstract class HuffmanTree implements Comparable<Weight>,Weight {
		public final long frequency; 
		public final int id;
		public HuffmanTree(long freq) { 
			frequency = freq; 
			id = nodeId+=2;
		}
		
		public int compareTo(Weight w) {
			return Long.compare(frequency, w.getWeight());
		}

		public long getWeight() {
			return frequency;
		}
	}

	class HuffmanLeaf extends HuffmanTree {
		public final byte[] val; 
		public HuffmanLeaf(long weight, byte[] dat) {
			super(weight);
			val = dat;
		}

	}

	class HuffmanNode extends HuffmanTree {
		public final HuffmanTree left, right; 
		public HuffmanNode(HuffmanTree lef, HuffmanTree rit) {
			super(lef.frequency + rit.frequency);
			left = lef;
			right = rit;
		}
	}

	private void switchFields(boolean toTmp) {
		if(toTmp) {
			tmpsymbol2Code = symbol2Code;
			tmpCodeIdx2Symbols = codeIdx2Symbols;
			tmpCodeValues = codeValues;
		} else  {
			defsymbol2Code = symbol2Code;
			defCodeValues = codeValues;
			defCodeIdx2Symbols = codeIdx2Symbols;
			defsymbl2CodLstIdx = codeIdx2Symbols.length-1;
			symbol2Code = tmpsymbol2Code;
			codeIdx2Symbols = tmpCodeIdx2Symbols;
			codeValues = tmpCodeValues;
			symbl2CodLstIdx = tmpsymbol2Code.length-1;
			tmpsymbol2Code =null;
			tmpCodeIdx2Symbols = null;
			tmpCodeValues = null;
		}
	}

	class ByteAryLst {
		byte[] ary;
		public int len = 0;
		int capacity;
		public ByteAryLst(int size) {
			ary = new byte[size];
			capacity = ary.length;
		}
		public void add(byte e) {
			ary[len] = e;
			if(++len == capacity) {
				ary = Arrays.copyOf(ary, ary.length*2);
				capacity = ary.length;
			}
		}
		public byte get(int idx) { return ary[idx];}
		public byte[] toAry() {
			return Arrays.copyOf(ary,len);
		}
	}

	private byte[] createCode(byte numBits, int index) {
		byte[] out = new byte[((numBits+7)/8)+1];
		out[0] = numBits;
		int bits = 0;
		for(int i=1; i<out.length; i++) {
			out[i] = (byte)(index >>> bits);
			bits +=8;
		}
		return out;
	}
	/**
	 * 
	 * @param in
	 * Stored HuffData from getHuffData()
	 */
	public CompressHuffman(byte[] in) {
		int inIdx = 29, count, hashIdx, codeIdx;
		byte[] symbol;
		Inflater inflater = new Inflater();  
		inflater.setInput(in);
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream(in.length*2); 
		byte[] buffer = new byte[8192];
		try {
			while (!inflater.finished()) {  
				count = inflater.inflate(buffer);
				outputStream.write(buffer, 0, count);
			}
			in =  outputStream.toByteArray();
		}catch(Exception e) {
			System.err.println("ERROR: unzipping CompressHuffman data failed!");
			e.printStackTrace();
		}
		totalSymbols = byteArrayToInt(in,0);
		symbl2CodLstIdx = byteArrayToInt(in,4);
		maxSymbolLength= byteArrayToInt(in,8);
		altCodeIdx =  byteArrayToInt(in,12);
		useAltOnly = in[16] == 1 ? true : false;
		highestLevel =  byteArrayToInt(in,17);
		altCodeBytes = byteArrayToInt(in,21);
		codeIdx2Symbols = new byte[byteArrayToInt(in,25)][][];
		symbol2Code = new byte[symbl2CodLstIdx+1][];
		codeValues = new byte[symbol2Code.length][];
		for(int i=1; i<codeIdx2Symbols.length; i++) {
			count = byteArrayToInt(in,inIdx);
			inIdx+=4;
			codeIdx2Symbols[i] = new byte[1 << i][];
			for(int w=0; w<count; w++) {
				symbol = new byte[in[inIdx++]];
				inIdx = bytesFromByteArray(in,symbol,inIdx);
				codeIdx = byteArrayToInt(in,inIdx);
				inIdx+=4;
				codeIdx2Symbols[i][codeIdx] = symbol;
				hashIdx = byteArrayToInt(in,inIdx);
				inIdx+=4;
				symbol2Code[hashIdx] = symbol;
				codeValues[hashIdx] = createCode((byte)i,codeIdx);
			}
		}
		switchFields(true);
		trees = new PriorityQueue<Weight>();
		buildAltTree(false);
	}
	/**
	 * Retrives the HuffmanTree data for offline storage 
	 * Feed the returned byte array into new CompressHuffman(byte[]) when you need to compress/decompress after program exit
	 * Don't call this method before the HuffTree has been generated
	 * @return
	 * The HuffmanTree and other internal datastructures
	 */
	public byte[] getHuffData() {
		byte[] out = new byte[(totalSymbols*(maxSymbolLength*10))*500];
		int count=0, hashIdx;
		byte[] aKey, symbol;
		int idx = 29;
		intToByteArray(totalSymbols,out,0);
		intToByteArray(symbl2CodLstIdx,out,4);
		intToByteArray(maxSymbolLength,out,8);
		intToByteArray(altCodeIdx,out,12);
		out[16] =  useAltOnly ? (byte)1 : 0;
		intToByteArray(highestLevel,out,17);
		intToByteArray(altCodeBytes,out,21);
		intToByteArray(codeIdx2Symbols.length,out,25);
		for(int i=1; i<codeIdx2Symbols.length; i++) {
			count = 0;
			for(int w=0; w<codeIdx2Symbols[i].length; w++) if(codeIdx2Symbols[i][w] != null) count++;
			idx = intToByteArray(count,out,idx);
			main: for(int w=0; w<codeIdx2Symbols[i].length; w++) {
				if(codeIdx2Symbols[i][w] != null) {
					if(out.length - idx < 1000) out = expand(out,1);
					out[idx++] = (byte) codeIdx2Symbols[i][w].length;
					idx = bytesToByteArray(codeIdx2Symbols[i][w],out,idx);
					idx = intToByteArray(w,out,idx);
					symbol = codeIdx2Symbols[i][w];
					hashIdx= aryHash(symbol) & symbl2CodLstIdx;
					while((aKey =symbol2Code[hashIdx]) != null) {
						if(Arrays.equals(aKey, symbol)) {
							idx = intToByteArray(hashIdx,out,idx);
							continue main;
						}
						if(++hashIdx == symbol2Code.length) hashIdx = 0;
					}
					System.err.println("CompressHuffman: a symbol not found when gettingHuffData");
				}
			}
		}
		out =  Arrays.copyOf(out, idx);
		Deflater deflater = new Deflater(); 
		deflater.setInput(out);  
		deflater.finish();  
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream(out.length);  
		byte[] buffer = new byte[8192];  
		while (!deflater.finished()) {  
			count = deflater.deflate(buffer);
			outputStream.write(buffer, 0, count);
		}
		return outputStream.toByteArray();
	}

	private static int byteArrayToInt(byte [] b, int startIdx) {
		return (b[startIdx] << 24) + ((b[startIdx+1] & 0xFF) << 16) + ((b[startIdx+2] & 0xFF) << 8) + (b[startIdx+3] & 0xFF);
	}

	private static int intToByteArray(int value, byte[] array, int startIdx) {
		array[startIdx] = (byte)(value >>> 24);
		array[startIdx+1] = (byte)(value >>> 16);
		array[startIdx+2] = (byte)(value >>> 8);
		array[startIdx+3] = (byte)value;
		return startIdx + 4;
	}

	private static int bytesToByteArray(byte[] values , byte[] array, int startIdx) {
		System.arraycopy(values, 0, array, startIdx, values.length);
		return startIdx + values.length;
	}

	private static int bytesFromByteArray(byte[] src, byte[] dest, int idx) {
		System.arraycopy(src, idx, dest, 0, dest.length);
		return idx + dest.length;
	}
	
    /**
     * <p>
     * Calculates XXHash64 from given {@code byte[]} buffer.
     * </p><p>
     * This code comes from <a href="https://github.com/jpountz/lz4-java">LZ4-Java</a> created
     * by Adrien Grand.
     * </p>
     *
     * @param buf to calculate hash from
     * @param off offset to start calculation from
     * @param len length of data to calculate hash
     * @param seed  hash seed
     * @return XXHash.
     */
    public static long hash(byte[] buf, int off, int len, long seed) {
        if (len < 0) {
            throw new IllegalArgumentException("lengths must be >= 0");
        }
        if(off<0 || off>=buf.length || off+len<0 || off+len>buf.length){
            throw new IndexOutOfBoundsException();
        }

        final int end = off + len;
        long h64;

        if (len >= 32) {
            final int limit = end - 32;
            long v1 = seed + PRIME64_1 + PRIME64_2;
            long v2 = seed + PRIME64_2;
            long v3 = seed + 0;
            long v4 = seed - PRIME64_1;
            do {
                v1 += readLongLE(buf, off) * PRIME64_2;
                v1 = Long.rotateLeft(v1, 31);
                v1 *= PRIME64_1;
                off += 8;

                v2 += readLongLE(buf, off) * PRIME64_2;
                v2 = Long.rotateLeft(v2, 31);
                v2 *= PRIME64_1;
                off += 8;

                v3 += readLongLE(buf, off) * PRIME64_2;
                v3 = Long.rotateLeft(v3, 31);
                v3 *= PRIME64_1;
                off += 8;

                v4 += readLongLE(buf, off) * PRIME64_2;
                v4 = Long.rotateLeft(v4, 31);
                v4 *= PRIME64_1;
                off += 8;
            } while (off <= limit);

            h64 = Long.rotateLeft(v1, 1) + Long.rotateLeft(v2, 7) + Long.rotateLeft(v3, 12) + Long.rotateLeft(v4, 18);

            v1 *= PRIME64_2; v1 = Long.rotateLeft(v1, 31); v1 *= PRIME64_1; h64 ^= v1;
            h64 = h64 * PRIME64_1 + PRIME64_4;

            v2 *= PRIME64_2; v2 = Long.rotateLeft(v2, 31); v2 *= PRIME64_1; h64 ^= v2;
            h64 = h64 * PRIME64_1 + PRIME64_4;

            v3 *= PRIME64_2; v3 = Long.rotateLeft(v3, 31); v3 *= PRIME64_1; h64 ^= v3;
            h64 = h64 * PRIME64_1 + PRIME64_4;

            v4 *= PRIME64_2; v4 = Long.rotateLeft(v4, 31); v4 *= PRIME64_1; h64 ^= v4;
            h64 = h64 * PRIME64_1 + PRIME64_4;
        } else {
            h64 = seed + PRIME64_5;
        }

        h64 += len;

        while (off <= end - 8) {
            long k1 = readLongLE(buf, off);
            k1 *= PRIME64_2; k1 = Long.rotateLeft(k1, 31); k1 *= PRIME64_1; h64 ^= k1;
            h64 = Long.rotateLeft(h64, 27) * PRIME64_1 + PRIME64_4;
            off += 8;
        }

        if (off <= end - 4) {
            h64 ^= (readIntLE(buf, off) & 0xFFFFFFFFL) * PRIME64_1;
            h64 = Long.rotateLeft(h64, 23) * PRIME64_2 + PRIME64_3;
            off += 4;
        }

        while (off < end) {
            h64 ^= (buf[off] & 0xFF) * PRIME64_5;
            h64 = Long.rotateLeft(h64, 11) * PRIME64_1;
            ++off;
        }

        h64 ^= h64 >>> 33;
        h64 *= PRIME64_2;
        h64 ^= h64 >>> 29;
        h64 *= PRIME64_3;
        h64 ^= h64 >>> 32;

        return h64;
    }
    
    static long readLongLE(byte[] buf, int i) {
        return (buf[i] & 0xFFL) | ((buf[i+1] & 0xFFL) << 8) | ((buf[i+2] & 0xFFL) << 16) | ((buf[i+3] & 0xFFL) << 24)
                | ((buf[i+4] & 0xFFL) << 32) | ((buf[i+5] & 0xFFL) << 40) | ((buf[i+6] & 0xFFL) << 48) | ((buf[i+7] & 0xFFL) << 56);
    }
    
    static int readIntLE(byte[] buf, int i) {
        return (buf[i] & 0xFF) | ((buf[i+1] & 0xFF) << 8) | ((buf[i+2] & 0xFF) << 16) | ((buf[i+3] & 0xFF) << 24);
    }
    
    public static int longHash(long h) {
        //$DELAY$
        h = h * -7046029254386353131L;
        h ^= h >> 32;
        return (int)(h ^ h >> 16);
    }
}

