# 探索Spark Tungsten的秘密 #

Spark Tungsten是databricks近期提出来的提升Spark性能的最新计划。 我们知道由于Spark是由scala开发的，JVM的实现带来了一些性能上的限制和弊端（例如GC上的overhead），使得Spark在性能上无法和一些更加底层的语言（例如c，可以对memory进行高效管理，从而利用hardware的特性）相媲美。 基于此，Tungsten就诞生了，从memory和cpu层面对spark的性能进行优化。 该项目的官方介绍详见[https://databricks.com/blog/2015/04/28/project-tungsten-bringing-spark-closer-to-bare-metal.html](https://databricks.com/blog/2015/04/28/project-tungsten-bringing-spark-closer-to-bare-metal.html "Project Tungsten: Bringing Spark Closer to Bare Metal")。 Tungsten的优化主要包括三个方面：memory management and binary processing，cache-aware computation， code generation。 官方已经对各个方面可以做的优化进行了介绍，这里我们将走进各个方面，详细介绍工作原理，优化细节以及代码实现。

## 概述 ##
官方对于Tungsten的三方面优化的概述

1. Memory Management and Binary Processing: application显示的对内存进行高效的管理以消除JVM对象模型和垃圾回收的开销；
2. Cache-aware computation：设计算法和数据结构以充分利用memory hierarchy
3. Code generation：使用code generation去充分利用最新的编译器和CPUs的性能

## Memory Management and Binary Processing ##
Tungsten设计了一套内存管理机制，而不再是交给JVM托管，Spark的operation直接使用分配的binary data而不是JVM objects。 这个章节，我们先详细介绍Tungsten的内存管理机制，然后分析下spark上的一些常用的operation如何基于新的内存管理机制实现它们的功能。

### 新内存管理机制诞生的Motivation ###

催生Tungsten内存管理机制的原因主要是JVM在内存方面的overhead。

1. JVM object model内存开销

	官方网站给出了一个很简单的字符串“abcd”的JVM object layout。
	
	    java.lang.String object internals:
		OFFSET  SIZE   TYPE DESCRIPTION                VALUE
	     0     4        (object header)                ...
	     4     4        (object header)                ...
	     8     4        (object header)                ...
	    12     4 char[] String.value                   []
	    16     4    int String.hash                    0
	    20     4    int String.hash32                  0
		Instance size: 24 bytes (reported by Instrumentation API)
	
	一个简单的4 bytes的字符串在JVM object model中居然需要48 bytes的内存。 可见我们需要抛弃JVM object，而像C语言那样直接操作分配的binary data。

2. Garbage collection的开销


GC的开销在所有居于JVM的application中都是不可忽视的并且tuning也十分繁琐，作为一个高效的In-memory processing framework，很多基于jvm的work都已经在底层直接写内存管理模块。 同理，spark想要想要在性能上有突破，需要对memory进行高效管理。

### Tungsten的内存管理机制 ###

内存管理必然涉及寻址，在介绍Tungsten的内存管理机制之前，我们先来看看内存地址的表示方式。 memory地址的表示方式在不同的内存管理模式中是不同的，这里我们考虑2种内存管理模式。第一种是off-heap模式，可以理解为是独立于JVM托管的heap之外利用c-style的malloc从os分配到的memory。这类memory不再由JVM托管，而是类似与c语言的内存管理，可以显示的在分配到的binary datga上进行操作而不是操作java object。由于不再由JVM托管，通过高效的内存管理，可以避免JVM object overhead和Garbage collection的开销。 在Spark中，上面描述的off-heap模式下c-style的memory访问由JVM的sun.misc.Unsafe包提供，例如显示的分配内存，释放以及内存指针。与c语言类似，在off-heap模式下，memory的地址可以简单的由一个64-bits的long类型表示。另外一个是on-heap模式，所有的Java objects都属于该模式。在该模式下，内存的地址由一个64bits的object引用和一个64bits的在该object内部的offset共同表示。

给定内存地址，就和c中的指针一样，我们可以在一个数据结构中指向另外一个数据结构。这对于off-heap模式是ok的，可是在on-heap模式中，由于GC会导致heap结构重新组织，java object的地址不是固定不变的。因此Tungsten利用了类似于os的page table结构对memory进行管理。如下图所示，memory被划分为多个页，内存寻址即是定位相应的内存页和在该页内的偏移。由于物理地址可能在动态的变化，所以需要一个类似于os虚拟内存的逻辑地址然后将逻辑地址映射到实际的物理地址。逻辑地址由一个64bits的long表示，地址的高13bits表示页编号，低51bits表示在改页内部的偏移。映射的过程，实际就是根据页编号去查询页表，然后得到该页的物理地址，然后在物理页地址上加上偏移就得到内存物理地址（这里说的虚拟和物理是在应用层面上进行的又一次抽象，不同于os的虚拟和物理地址，但是思想是一致的）。

![Memory Management](https://github.com/hustnn/TungstenSecret/blob/master/images/memory-tungsten.png)

对于off-heap和inheap模式，上述虚拟地址表示是一样的，可是物理地址是不同的。在off-heap模式，内存通过一个64bits的绝对内存地址表示。在on-heap模式，内存由相对于一个JVM对象的偏移表示。为了将2者统一起来，定义了一个MemoryLocation来表示off-heap和in-heap的内存地址。MemoryLocation的实现如下所示,对于off-heap的memory，obj为null，offset则为绝对的内存地址，对于on-heap的memory，obj则是JVM对象的基地址，offset则是相对于改对象基地址的偏移。

    /**
	 * A memory location. Tracked either by a memory address (with off-heap allocation),
	 * or by an offset from a JVM object (in-heap allocation).
	 */
	public class MemoryLocation {
	
	  @Nullable
	  Object obj;
	
	  long offset;
	  
	  public MemoryLocation(@Nullable Object obj, long offset) {
	    this.obj = obj;
	    this.offset = offset;
	  }
	
	  public MemoryLocation() {
	    this(null, 0);
	  }
	
      /*内存寻址就调用该函数，设置obj和offset*/
	  public void setObjAndOffset(Object newObj, long newOffset) {
	    this.obj = newObj;
	    this.offset = newOffset;
	  }
	
	  public final Object getBaseObject() {
	    return obj;
	  }
	
	  public final long getBaseOffset() {
	    return offset;
	  }
	}

Tungsten基于上图page table的内存管理主要由TaskMemoryManager实现。这里我主要分析几个主要的功能。
	
	这里调用ExecutorMemoryManager进行内存分配，分配得到一个内存页，将其添加到
	page table中，以遍内存地址映射
    /**
	   * Allocate a block of memory that will be tracked in the MemoryManager's page table; this is
	   * intended for allocating large blocks of memory that will be shared between operators.
	   */
	  public MemoryBlock allocatePage(long size) {
	    if (size > MAXIMUM_PAGE_SIZE_BYTES) {
	      throw new IllegalArgumentException(
	        "Cannot allocate a page with more than " + MAXIMUM_PAGE_SIZE_BYTES + " bytes");
	    }
	
	    final int pageNumber;
	    synchronized (this) {
	      pageNumber = allocatedPages.nextClearBit(0);
	      if (pageNumber >= PAGE_TABLE_SIZE) {
	        throw new IllegalStateException(
	          "Have already allocated a maximum of " + PAGE_TABLE_SIZE + " pages");
	      }
	      allocatedPages.set(pageNumber);
	    }
	    final MemoryBlock page = executorMemoryManager.allocate(size);
	    page.pageNumber = pageNumber;
	    pageTable[pageNumber] = page;
	    if (logger.isTraceEnabled()) {
	      logger.trace("Allocate page number {} ({} bytes)", pageNumber, size);
	    }
	    return page;
	  }
	 
	 给定分配到的内存页和页内的偏移，生成一个64bits的逻辑地址
	 /**
	   * Given a memory page and offset within that page, encode this address into a 64-bit long.
	   * This address will remain valid as long as the corresponding page has not been freed.
	   *
	   * @param page a data page allocated by {@link TaskMemoryManager#allocate(long)}.
	   * @param offsetInPage an offset in this page which incorporates the base offset. In other words,
	   *                     this should be the value that you would pass as the base offset into an
	   *                     UNSAFE call (e.g. page.baseOffset() + something).
	   * @return an encoded page address.
	   */
	  public long encodePageNumberAndOffset(MemoryBlock page, long offsetInPage) {
	    if (!inHeap) {
	      // In off-heap mode, an offset is an absolute address that may require a full 64 bits to
	      // encode. Due to our page size limitation, though, we can convert this into an offset that's
	      // relative to the page's base offset; this relative offset will fit in 51 bits.
	      offsetInPage -= page.getBaseOffset();
	    }
	    return encodePageNumberAndOffset(page.pageNumber, offsetInPage);
	  }
	
      高13bits是page number，低位为页内偏移
	  @VisibleForTesting
	  public static long encodePageNumberAndOffset(int pageNumber, long offsetInPage) {
	    assert (pageNumber != -1) : "encodePageNumberAndOffset called with invalid page";
	    return (((long) pageNumber) << OFFSET_BITS) | (offsetInPage & MASK_LONG_LOWER_51_BITS);
	  }
	
      给定逻辑地址，获取page number
	  @VisibleForTesting
	  public static int decodePageNumber(long pagePlusOffsetAddress) {
	    return (int) ((pagePlusOffsetAddress & MASK_LONG_UPPER_13_BITS) >>> OFFSET_BITS);
	  }
	
      给定逻辑地址，获取页内偏移
	  private static long decodeOffset(long pagePlusOffsetAddress) {
	    return (pagePlusOffsetAddress & MASK_LONG_LOWER_51_BITS);
	  }
	
      给定
	  /**
	   * Get the page associated with an address encoded by
	   * {@link TaskMemoryManager#encodePageNumberAndOffset(MemoryBlock, long)}
	   */
	  public Object getPage(long pagePlusOffsetAddress) {
	    if (inHeap) {
	      final int pageNumber = decodePageNumber(pagePlusOffsetAddress);
	      assert (pageNumber >= 0 && pageNumber < PAGE_TABLE_SIZE);
	      final MemoryBlock page = pageTable[pageNumber];
	      assert (page != null);
	      assert (page.getBaseObject() != null);
	      return page.getBaseObject();
	    } else {
	      return null;
	    }
	  }
	
	  /**
	   * Get the offset associated with an address encoded by
	   * {@link TaskMemoryManager#encodePageNumberAndOffset(MemoryBlock, long)}
	   */
	  public long getOffsetInPage(long pagePlusOffsetAddress) {
	    final long offsetInPage = decodeOffset(pagePlusOffsetAddress);
	    if (inHeap) {
	      return offsetInPage;
	    } else {
	      // In off-heap mode, an offset is an absolute address. In encodePageNumberAndOffset, we
	      // converted the absolute address into a relative address. Here, we invert that operation:
	      final int pageNumber = decodePageNumber(pagePlusOffsetAddress);
	      assert (pageNumber >= 0 && pageNumber < PAGE_TABLE_SIZE);
	      final MemoryBlock page = pageTable[pageNumber];
	      assert (page != null);
	      return page.getBaseOffset() + offsetInPage;
	    }
	  }

### 基于Tungsten内存管理的应用 ###

unsafe.memory.TaskMemoryManager  按照pagetable的方式对heap或者off-heap的memory进行统一管理

使用到TaskMemoryManager的地方：

org.apache.spark:

- TaskContext
- TaskContextImpl

org.apache.spark.executor:

- Executor: 设置task的taskMemoryManager

org.apache.spark.scheduler:

-Task：成员taskMemoryManager的函数，构造TaskContextImpl

org.apache.spark.shuffle.unsafe:

- UnsafeShuffleExternalSorter
- UnsafeShuffleWriter

org.apache.spark.unsafe.map:

- BytesToBytesMap

org.apache.spark.util.collection.unsafe.sort:

- UnsafeExternalSorter
- UnsafeInMemorySorter

org.apache.spark.sql.execution:

- UnsafeFixedWidthAggregationMap
- UnsafeKVExternalSorter

org.apache.spark.sql.execution.joins:

- HashedRelation


# Tungsten unsafe的memory管理和binary processing的机制 #

Tungsten按照page table的方式对memory进行管理，每个page是一个MemoryBlock，这个block可以是heap memory或者off heap memory，它们之所有能统一是由MemoryBlock的父类MemoryLocation抽象实现，MemoryLocation要么用一个long的address表示off-heap的分配，要么用一个object+offset from a JVM object表示in-heap的分配. 在page table管理机制之下，所有memory的地址由page number和offsetInPage决定(off-heap memory使用的是绝对地址，off-heap page就使用一个绝对地址表示，page内offset是当前地址相对于page的绝对初始地址而言，on-heap就是相对地址，on-heap的page使用obj+jvm object的初始offset表示，page内的offset就是相对于jvm object初始offset而言）。在task或者operator之间对memory进行传递，都统一按照这个统一方式去寻址。给定一个long的address，可以按照前面encode的方式decode出对应的地址，即定位到相应的page table和在该page内的offset(对于off-heap memory则是还原出绝对地址，对于on-heap memory则是还原出object和jvm object offset）。

# 应用 #
*************************
- Unsafe HashMap

UnsafeFixedWidthAggregationMap的实现，使用了一个hash map which maps from opaque bytearray keys to bytearray values，我们先来分析这个hash map，BytesToBytesMap.

BytesToBytesMap：这是一个利用Tungsten unsafe memory管理机制实现的hash table，key和value一起存储，结构如下：
Bytes 0 to 4: len(k) (key length in bytes) + len(v) (value length in bytes) + 4
Bytes 4 to 8: len(k)
Bytes 8 to 8 + len(k): key data
Bytes 8 + len(k) to 8 + len(k) + len(v): value data
其调用方法为首先调用hash map的lookup方法来定位location，然后调用putNewKey更新hash table。 Lookup原理就是普通的hash map的查询，key就是unsafe memory的类型，由keyBaseObject，KeyBaseOffset还有一个key的length。 首先检查bitset，如果没有set则为new key，否则比较hashcode，若hashcode不匹配即冲突则探测新位置，若hashcode完全匹配则去unsafe memory里面读出完整的key进行比较。 最后返回该key是否被定义。若定义则取出value进行处理，若未定义，则调用putNewKey插入key和value。


使用该unsafe hash map的类: 

1. UnsafeFixedWidthAggregationMap (ok)
2. UnsafeKVExternalSorter
3. HashedRelation

UnsafeFixedWidthAggregationMap:

通过groupingKeyRow找到对应的hash table中的value，作aggregation;

iterate hash table.

UnsafeKVExternalSorter:

in-place sorting of records in the hash map??

HashedRelation:

HashedRelation for UnsafeRow, which is backed by HashMap or BytesToBytesMap (better memory performance).

**************************
- Unsafe shuffle/sorting

最关键的几个类：UnsafeShuffleManager,UnsafeShuffleExternalSorter,UnsafeShuffleInMemorySorter,UnsafeShuffleWriter。


UnsafeShuffleManager:
能使用的UnsafeShuffleManager的优化的条件：
shuffle之后没有aggregation或者ordering
shuffle serializer支持serialized values的relocation
shuffle产生的partitions少于16777216
单条record小于128MB

UnsafeShuffleManager的工作原理和SortShuffleManager类似。在sort-based shuffle中，map的records按照他们的partition id进行排序，然后写到一个map output file中。 Reducers读取这个文件中连续的区域。 当Map output不能fit在memory中时候，sorted subsets会被spill到disk上，之后再merge成一个文件。

UnsafeShuffleManager的优化：

直接在serialized binary data上sort而不是java objects，减少了memory的开销和GC的overhead。这个优化需要record serializer能支持serialized的records无需deserialization而重新排序；

使用cache-efficient sorter（UnsafeShuffleExternalSorter），能够对compressed的record pointers和partition ids的数组进行排序。每条记录只有8 bytes，更多的data能容纳在cache中；

Spill merging能够可操作相同partition的serialized的records并且在merge的时候无需deserialize这些records；

当spill compression codec支持连接compressed data，Spill merge连接serialized并且压缩的spill partitions以得到final output partition。 这里使用高效的data copying 方法，例如NIO's transferTo，来避免在merge过程中allocate decompression或者copying buffer。 

UnsafeShuffleWriter:
将每条record写入到UnsafeShuffleExternalSorter中，由于所有data并不能都能保存到memory，该sorter会在产生多个sorted的spill的文件，当所有record写完时，将所有的spill进行merge。 这里有不同的merge策略，不同merge策略对应不同的IO技术。



UnsafeShuffleExternalSorter:
利用前面介绍的按照类似于os page table结构的unsafe memory manager进行内存管理。需要进行shuffle的data往往比较大，不能存储在memory中，所以需要spill。 判断需不需要spill利用ShuffleMemoryManager.tryToAcquire来决定。如果spill之后依然不能满足，则表示申请的memory过大，抛出异常。Spill前需要先在memory中对records进行排序，然后再写到disk。 这里主要介绍如何利用unsafe memory manager进行sort。当有新的record来，则从unsafe的TaskMemoryManager allocatePage来存储改record，如果当前page满了，则重新分配。 如果分配成功，则record成功保存在某page相应的offset的位置。通过TaskMemoryManager计算出在page table架构下对应的record address。 Record之后则利用该address去page table中获取其内容。 Spill中对record在Memory中排序由UnsafeShuffleInMemorySorter实现，每条record对应<record address, partition id>，在sort的时候只需要对partition id进行排序，sort之后，利用record address去查询record的值，然后write到disk。


UnsafeShuffleInMemorySorter：
如上面所说，每条record由<record address, partition ID>表示，然后该vector由PackedRecordPointer ecode为long类型。 Sort操作将直接作用于一个long的array而不是实际的records。Sort之后返回有序的long array的iterator。


**********************
- Unsafe Join

to do

**********************
- Unsafe Aggregation
涉及的类TungstenAggregate，TungstenAggregationIterator，UnsafeFixedWidthAggregationMap。

TungstenAggregationIterator：使用一个hash map（UnsafeFixedWidthAggregationMap）来保存所有groups和它们对应的aggregation buffers.

UnsafeFixedWidthAggregationMap: 使用BytesToBytes HashMap来对定长的value作aggregation操作。 每个group key对应一个aggregation buffer。 若为新key，则插入一个新的aggregation buffer到hash map中，若该key已经存在，则取出对应的aggregation buffer。



# Tungsten Cache-aware Computation 机制 #
Cache-aware computation主要是相对于In-memory computation，L1/L2/L3 CPU caches比memory速度快可是size更小。那么就需要设计对cache友好的数据结构，提高cache hit和cache locality。

以前面应用中的UnsafeShuffleSort，对records进行sort为例，传统的做法是每个record有个指针指向该record<key,value>，对record排序就是比较指针指向的key，然后进行比较，这个操作涉及的都是random的memory access，cache locality会变得很低。 Tungsten里面的sort则是将key和record指针放在一起，排序操作则是按照线性方式查询key-pointer对，避免的memory的随机访问。

如何利用cache优化aggregations， sorting和join操作的效率？study Unsafe Join和Unsafe Aggregation

# Tungsten Code Generation 机制 #

GenerateUnsafeProjection -- projects any internal row data structure directly into bytes (UnsafeRow).
ConvertToUnsafe将Java-object-based row转换为UnSafeRow，具体的转换则是在GenerateUnsafeProjection实现。 代码的生成同样也利用的Java Unsafe对memory直接操作来避免JVM object model的overhead。
