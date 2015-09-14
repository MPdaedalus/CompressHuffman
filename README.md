# CompressHuffman
CompressHuffman is a library to compress isolated records of data in a DB using a shared huffman tree generated from all the records.

 This library was created because compressing a single record in a DB using DEFLATE or other methods produces poor compression (5-10%) due to high entropy of individual records (due to small size). 
 
 Compress huffman exploits the low entropy of the entire dataset to produce a huffman tree that can compress most individual records with 30-70% compression (depending on data). 

This library is free to use and is under the apache 2.0 licence, available @ https://www.apache.org/licenses/LICENSE-2.0

# Usage

Feed your dataset byte[record][recordData] for an in memory dataset or Iterable byte[recordData] for retrieval from DB to the Constructor.

You can then use compress(byte[]) decompress(byte[])  after the HuffmanTree has been generated.

To Store the HuffmanTree for future use,  use getHuffData() and store it in a file.

Note: once you generate the huffTree (huffData) and perform compress operations, that huffTree is required to decompress the data, you will not be able to decompress at a later date without it. Generating a new hufftree will not
work as the huffTree generation process is not deterministic and will result in an incompatible hufftree.

Read and feed the huffData byte array to new CompressHuffman(byte[]) at a future date then you can use  compress(byte[]) decompress(byte[]) methods.

The default configuration produces a middle of the road compromise between compression level and compress/decompress speed.

If you want maximum compress/decompress speed at the expense of compression level (10-20% lower) then you can use>

CompressHuffman ch = new CompressHuffman(data,CompressHuffman.fastestCompDecompTime());

If you want maximum compression at the expense of compression/decompression speed (10-20MB/s slower) you can use>

CompressHuffman ch = new CompressHuffman(data,CompressHuffman.smallestFileSize());

These presets should be fine for most but you can tune the individual internal parameters such as symbol size and max symbols
using>

CompressHuffman ch = new CompressHuffman(data, CompressHuffman.config(int maxSymbolLength, int maxSymbols, long maxSymbolCacheSize, boolean twoPass, int maxTreeDepth)

Note: you can compress/decompress new data (that was not part of the dataset during huffTree creation). 

If the distribution and frequency of the symbols in this new data are similar to the distribution and frequency of the
dataset used to generate the huffTree then the compression level will be equally similar.

However if the symbols are different or their frequency is different you will get little to no compression and the records can even be BIGGER due to not having the available symbols in the hufftree and using extra bits to flag uncompressed data.

Using the VM option -XX:+UseCompressedOops can speed things up by about 10% as long as your heap is <32GB.

# How it works

CompressHuffman works by finding all the unique symbols in a datset along with their frequency.
The algorithm then discards all symbols that have a score below 3 (score = freq*symbol Length). 

The list of symbols is then capped at a desired number of symbols, eg 10000, 100000, 1 million, to prevent the hufftree from taking up too many bytes and to speed up compression/decompression.

A huffman tree is then built from this sorted list of symbols resutling in the highest scoring symbols having the fewest bits and the lowest scoring symbols have the most bits. 

There is also an AltNode added which is a zero length symbol that acts as a flag to tell the Compressor/Decompressor 
to use alternate compression (using a list of chars with its own hufftree), or if the symbol
to be compressed is not present in either the main huffman tree or the alternate huffman tree then just add it uncompressed.

Because a huffman tree is a binary tree, (0=go left branch, 1=go right branch) navigating it is slow due the CPUs internal branch predictor getting it wrong half the time meaning the piplining speedups that cpus use are ineffective.

To rectify this problem the tree is eliminated and converted to a series of arrays in the following structure

 codeIdx2Symbols[bitLen][symbolIdx]
 
 The codes outputed by the compressor are nothing but a series of variable bit length indexes to the codeIdx2Symbols array.
 
 The decompresor simply adds each bit in the compressed record to an int (symbolIdx) checking codeIdx2Symbols each time to see if a symbol  exists at that index.
 
 This structure does not result in unpredictable branching and so allows the CPU to exploit pipelining resulting in a speed up.
 
 There  is also a twopass option (disabled by default) in config() that will create a dummy tree and do a dummy compress on each record to find out which symbols are never used.
 
 These symbols are then eliminated and a final huffTree built.
 
 This improves compression by a few % but building the huffTree takes more than twice as long as is needs to iterate over the dataset twice and  dummy compress each record.
 
 # Benchmark
 
 Here is the results of a benchmark where the dataset was 5.2 million records each containing a sentence from a random wikipedia article. Total Dataset size 594.6MB

Using default config> new CompressHuffman(data);

HuffTree Generation Time: 219 seconds

Total compressTime: 59 sec, Total DecompressTime: 13 sec, Total Size of all compressed records> 269.6MB

Using> new CompressHuffman(data,CompressHuffman.fastestCompDecompTime());

HuffTree Generation Time: 281 seconds

Total compressTime: 48 sec, Total DecompressTime: 11 sec, Total Size of all compressed records> 322.6MB

Using> new CompressHuffman(data,CompressHuffman.smallestFileSize());

HuffTree Generation Time:273 seconds

Total compressTime: 98 sec, Total DecompressTime: 17 sec, Total Size of all compressed records> 236.6MB

Decompression speed  is about 50MB/sec per core when using fastestCompDecompTime() and 35MB/sec when using smallestFileSize().

This was tested on a intel sandybridge 4 core i5 (no HT) 6MB L3 cache CPU @ 4.5ghz, 1866mhz ram.

You will probably get better performance on CPU with more L3 cache as more of the most used symbols will stay there rather than going to main memory.
