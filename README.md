## Introduction
[📘 中文版文档](./docs/README_ZH.md)

**bricksKV** is a high-performance key-value storage engine implemented in Rust.  
In simple terms, bricksKV can be seen as a **disk-based version of ConcurrentHashMap** — it supports serial writes and concurrent reads, achieving **O(1)** time complexity for reads.

The core design concept separates the storage of **keys** and **values**:

1. **Keys** are distributed into different index files based on their hash values. Each index entry stores both the key and the position of its corresponding value.
2. **Values** are stored in tiered files according to their size. Each file supports a fixed record length (e.g., 32 B, 64 B, 128 B, etc.). Each value has an ID marking its storage position.  
   During reads, the system locates the appropriate key bucket based on the key’s hash, retrieves the value ID, then directly locates and reads the corresponding value file.

---

## Design philosophy

- **Key storage**  
  LevelDB suffers from slow read performance because locating a key involves multiple steps: finding the right level, locating the correct file, opening it, reading the file index, and finally retrieving the value — potentially repeating this process across multiple levels.

  In contrast, bricksKV takes a **hashmap-like** approach to store keys. Since file storage cannot directly use in-memory arrays or linked lists, bricksKV handles hash collisions by probing a limited number of slots (e.g., up to 32) after the hashed index.  
  If collision resolution fails, the bucket is expanded. Using multiple buckets (e.g., 8192) reduces both the probability of collisions and the frequency of resizing operations.

- **Value storage**  
  Write amplification in systems like LevelDB often arises from variable-length values that complicate resource management and require costly compaction or merging operations.  
  To address this, bricksKV uses **fixed-length data pages**, which greatly simplify allocation and deallocation.  
  Although real-world values are often variable in length, this issue is mitigated by using multiple tiers of fixed-length files — e.g., 32 B, 64 B, 128 B, 256 B, 512 B, 1024 B, 2048 B, 4096 B, etc.

  Each tier uses **multi-level bitmaps** to manage data page allocation and release.  
  Each bit in an upper-level bitmap manages 8 bits in the level below:
  - `0` → indicates available space
  - `1` → indicates full occupancy

---

## Architecture

![Architecture](./docs/image/architecture.png)

### Persistent layer
- **WAL (Write-Ahead Log)**  
  Appends key-value pairs to disk for durability and crash recovery.
- **Key store**  
  Stores keys in buckets based on their hash values.
- **Value store**  
  Manages fixed-length data page files categorized by value size.

### Memory layer
- **KV buffer**  
  Temporarily holds newly written key-value pairs in memory before persistence.
- **KV cache**  
  Caches frequently accessed key-value data from the persistent layer. The cache size is configurable to optimize performance.

---

## Core flow

![Core flow](./docs/image/core-flow.png)

- **Write path**  
  Data is first appended to the WAL, then written into the KV buffer, and finally into the KV cache.

- **Asynchronous flush**  
  When a WAL file exceeds a specified size (e.g., 4 MB), an asynchronous flush is triggered.  
  Each WAL file corresponds to one map in the KV buffer.  
  The system flushes each KV pair by first writing the value to the value store, then writing the key to the key store.  
  After flushing, the WAL file and its corresponding buffer map are deleted.

- **Read path**  
  Reads first check the KV buffer. If not found, the system falls back to the key store and value store.

---

## Core module design

### Persistent layer

#### WAL (Write-Ahead Log)

![wal](./docs/image/wal.png)

The WAL stores data in an append-only format consisting of a 4-byte length field followed by a payload.  
It does not interpret data contents — it only tracks record lengths for sequential persistence and recovery.

#### Value store

The value store manages multiple fixed-length data page files to handle values of different sizes.  
Each file is managed by multi-level bitmaps (`bitIndex`), where each bit in the upper level manages 8 bits in the lower level.  
A bit value of `0` indicates available space, while `1` indicates full occupancy.

Supported value sizes include **32 B, 64 B, 128 B, 256 B, 512 B, 1024 B, 2048 B, 4096 B**, etc.  
Values smaller than or equal to 32 B are stored in the 32 B file, those ≤ 64 B in the 64 B file, and so on.

Each data page has an incrementing **ID**, allowing direct offset calculation for fast reads.

Single-level data page file:  
![one-level](./docs/image/value-store.png)

Multi-level data page files:  
![multi-level](./docs/image/multi_level_data_page_file.png)

#### Key store

![key-store](./docs/image/key-store.png)

The key store adopts a **ConcurrentHashMap-style** structure: keys are hashed into buckets, and within each bucket, the hash determines the data index.  
Each record in the key store has a fixed size, containing:  
`key + value_id + value_length`.

Hash collisions are resolved using limited linear probing (e.g., up to 32 probes).  
If no free slot is found, the store expands — creating a larger file and migrating existing records, similar to a hashmap resize.

---

### Memory layer

#### KV buffer

The KV buffer ensures data consistency before flushing to the key store and value store — similar to **LevelDB’s memtable**.  
Each WAL file corresponds to one in-memory map in the buffer. Once flushed to disk, both the WAL file and the map are deleted.

#### KV cache

The KV cache sits between the buffer and the persistent stores. It caches key-value data to reduce disk I/O and improve read performance.  
As a cache, it includes an eviction policy (e.g., **LRU**).

---

## Performance analysis

- **Write**  
  Sequential appends to the WAL file ensure efficient disk writes.

- **Read**  
  Supports concurrent reads. If data exists in the KV cache, access is fast. Otherwise, the system locates the key through the key store using a hash lookup.  
  Even in the case of hash collisions, up to 32 probes (typically within a 4 KB region) are sufficient — often requiring only **one disk I/O**.  
  Then, using the value ID, another I/O retrieves the value from the value store.

  Thus, **in most cases, a read can be completed in just two I/O operations** — even without cache hits, while maintaining **O(1) time complexity** for lookups.