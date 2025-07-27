#ifndef KVALUE_MINHASH_H
#define KVALUE_MINHASH_H

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdatomic.h>

// xxHash32 implementation (optimized for speed)
#define XXH_PRIME32_1 0x9E3779B1U
#define XXH_PRIME32_2 0x85EBCA77U
#define XXH_PRIME32_3 0xC2B2AE3DU
#define XXH_PRIME32_4 0x27D4EB2FU
#define XXH_PRIME32_5 0x165667B1U

static inline uint32_t xxh32_rotl(uint32_t x, int r) {
    return (x << r) | (x >> (32 - r));
}

static inline uint32_t xxh32_hash(uint32_t input, uint32_t seed) {
    uint32_t h32 = seed + XXH_PRIME32_5 + 4;
    h32 += input * XXH_PRIME32_3;
    h32 = xxh32_rotl(h32, 17) * XXH_PRIME32_4;
    h32 ^= h32 >> 15;
    h32 *= XXH_PRIME32_2;
    h32 ^= h32 >> 13;
    h32 *= XXH_PRIME32_3;
    h32 ^= h32 >> 16;
    return h32;
}

// Static buffer pool
#define MAX_INSTANCES 4
#define MAX_K 1024

typedef struct {
    uint32_t k;          // Max capacity
    uint32_t count;      // Current count
    uint32_t space_size; // Hash space modulo
    uint32_t seed;       // Hash seed
    uint32_t *hashes;    // Sorted descending
} kvalue_minhash_t;

static struct {
    kvalue_minhash_t kmh;
    atomic_int in_use;  // Changed from int to atomic_int
    uint32_t hashes[MAX_K];
} kmh_pool[MAX_INSTANCES];

#define MAX_SERIALIZE_BUFFERS 4

typedef struct {
    uint8_t buffer[(MAX_K+5)*sizeof(uint32_t)];
    atomic_int busy;  // 0 = free, 1 = busy
    int heap_allocated; // 0 = pooled, 1 = heap allocated
} kmh_buffer_t;

static kmh_buffer_t kmh_buffer_pool[MAX_SERIALIZE_BUFFERS];

static inline kvalue_minhash_t* kmh_init(uint32_t k, uint32_t space_size, uint32_t seed) {
    // Try pool first
    if (k <= MAX_K) {
        for (int i = 0; i < MAX_INSTANCES; i++) {
            int expected = 0;
            if (atomic_compare_exchange_weak(&kmh_pool[i].in_use, &expected, 1)) {
                // Successfully acquired this instance
                kmh_pool[i].kmh.k = k;
                kmh_pool[i].kmh.count = 0;
                kmh_pool[i].kmh.space_size = space_size;
                kmh_pool[i].kmh.seed = seed;
                kmh_pool[i].kmh.hashes = kmh_pool[i].hashes;
                return &kmh_pool[i].kmh;
            }
        }
    }
    
    // Fallback to malloc
    kvalue_minhash_t *kmh = malloc(sizeof(kvalue_minhash_t) + k * sizeof(uint32_t));
    if (!kmh) return NULL;
    
    kmh->k = k;
    kmh->count = 0;
    kmh->space_size = space_size;
    kmh->seed = seed;
    // FIX: Set the hashes pointer to point to the memory allocated after the struct
    kmh->hashes = (uint32_t*)(kmh + 1);
    return kmh;
}

static inline void kmh_free(kvalue_minhash_t *kmh) {
    if (!kmh) return;
    
    // Check if it's from pool
    for (int i = 0; i < MAX_INSTANCES; i++) {
        if (&kmh_pool[i].kmh == kmh) {
            atomic_store(&kmh_pool[i].in_use, 0);
            return;
        }
    }
    // Must be malloc'd, free normally
    free(kmh);
}

/*
// Initialize MinHash
static inline kvalue_minhash_t* kmh_init(uint32_t k, uint32_t space_size, uint32_t seed) {
    kvalue_minhash_t *kmh = malloc(sizeof(kvalue_minhash_t) + k * sizeof(uint32_t));
    if (!kmh) return NULL;
        
    kmh->k = k;
    kmh->count = 0;
    kmh->space_size = space_size;
    kmh->seed = seed;
    return kmh;
}

// Free MinHash
static inline void kmh_free(kvalue_minhash_t *kmh) {
    if (kmh) {
        free(kmh);
    }
}

*/
// Add value (optimized for speed)
// Always keeps the K smallest hashes, stored in descending order.
static inline void kmh_add(kvalue_minhash_t *kmh, uint32_t value) {
    uint32_t hash = xxh32_hash(value, kmh->seed) % kmh->space_size;
    
    // Check for duplicates
    for (uint32_t j = 0; j < kmh->count; j++) {
        if (kmh->hashes[j] == hash) {
            return;
        }
    }

    // List not full yet
    if (kmh->count < kmh->k) {
        // Insert in sorted order (descending)
        uint32_t i = kmh->count;
        while (i > 0 && kmh->hashes[i-1] < hash) {
            kmh->hashes[i] = kmh->hashes[i-1];
            i--;
        }
        kmh->hashes[i] = hash;
        kmh->count++;
        return;
    }
    
    // List is full (kmh->count == kmh->k)
    // Check if the new hash is larger than the current largest of the K smallest
    if (hash >= kmh->hashes[0]) {
        return; // Not among the K smallest, discard
    }

    // Hash is smaller than current largest, so it must be included.
    // Shift all elements one position to the left, effectively dropping kmh->hashes[0].
    memmove(&kmh->hashes[0], &kmh->hashes[1], (kmh->k - 1) * sizeof(uint32_t));

    // Find the correct insertion point for the new hash in the shifted array
    uint32_t i = kmh->k - 1; // Start from the last valid index
    while (i > 0 && kmh->hashes[i-1] < hash) {
        kmh->hashes[i] = kmh->hashes[i-1];
        i--;
    }
    kmh->hashes[i] = hash; // Insert the new hash
    // kmh->count remains kmh->k
}

// Cardinality estimation
static inline double kmh_cardinality(const kvalue_minhash_t *kmh) {
    if (kmh->count == 0) return 0.0;
    if (kmh->count < kmh->k) {
        // Incomplete sketch - just use the current count
        return (double)kmh->count;
    }
    // Complete sketch - use k-th smallest hash
    return (double)kmh->space_size * (kmh->k - 1) / ( kmh->hashes[0] + 1 );
}

// Merge two MinHashes
static inline kvalue_minhash_t* kmh_merge(const kvalue_minhash_t *a, const kvalue_minhash_t *b) {
    if (a->k != b->k || a->space_size != b->space_size || a->seed != b->seed) return NULL;
    
    kvalue_minhash_t *result = kmh_init(a->k, a->space_size, a->seed);
    if (!result) return NULL;
    
    // Start from the end of both arrays (smallest values) and work backwards
    int i = a->count - 1;
    int j = b->count - 1;
    
    while (result->count < result->k && (i >= 0 || j >= 0)) {
        uint32_t hash;
        
        if (i < 0) {
            hash = b->hashes[j--];
        } else if (j < 0) {
            hash = a->hashes[i--];
        } else if (a->hashes[i] < b->hashes[j]) {
            hash = a->hashes[i--];
        } else if (a->hashes[i] > b->hashes[j]) {
            hash = b->hashes[j--];
        } else {
            // Equal values - take one and skip the other
            hash = a->hashes[i--];
            j--;
        }
        
        result->hashes[result->count++] = hash;
    }
    
    // Reverse the result array to maintain descending order
    for (uint32_t idx = 0; idx < result->count / 2; idx++) {
        uint32_t temp = result->hashes[idx];
        result->hashes[idx] = result->hashes[result->count - 1 - idx];
        result->hashes[result->count - 1 - idx] = temp;
    }
    
    return result;`
}

// Jaccard distance
static inline double kmh_distance(const kvalue_minhash_t *a, const kvalue_minhash_t *b) {
    if (a->k != b->k || a->space_size != b->space_size || a->seed != b->seed) return -1.0;
    
    uint32_t matches = 0;
    uint32_t i = 0, j = 0;
    uint32_t compared = 0;
    
    while (i < a->count && j < b->count && compared < a->k) {
        if (a->hashes[i] == b->hashes[j]) {
            matches++;
            i++; j++;
        } else if (a->hashes[i] > b->hashes[j]) {
            i++;
        } else {
            j++;
        }
        compared++;
    }
    
    return compared > 0 ? 1.0 - (double)matches / compared : 1.0;
}

// Varint encoding utilities
static inline uint32_t varint_encode(uint32_t value, uint8_t *buf) {
    uint32_t len = 0;
    while (value >= 0x80) {
        buf[len++] = (value & 0x7F) | 0x80;
        value >>= 7;
    }
    buf[len++] = value & 0x7F;
    return len;
}

static inline uint32_t varint_decode(const uint8_t *buf, uint32_t *value) {
    const uint8_t *p = buf;
    uint32_t result = 0;
    uint32_t shift = 0;
    
    while (*p & 0x80) {
        result |= (*p++ & 0x7F) << shift;
        shift += 7;
    }
    result |= *p++ << shift;
    *value = result;
    return p - buf;
}

// SQLite4 variable integer encoding utilities
static inline uint32_t sqlite4_encode(uint64_t value, uint8_t *buf) {
    if (value <= 240) {
        buf[0] = (uint8_t)value;
        return 1;
    }
    if (value <= 2287) {
        uint32_t v = (uint32_t)(value - 240);
        buf[0] = (v / 256) + 241;
        buf[1] = v % 256;
        return 2;
    }
    if (value <= 67823) {
        uint32_t v = (uint32_t)(value - 2288);
        buf[0] = 249;
        buf[1] = v / 256;
        buf[2] = v % 256;
        return 3;
    }
    if (value <= 0xFFFFFF) {
        buf[0] = 250;
        buf[1] = (value >> 16) & 0xFF;
        buf[2] = (value >> 8) & 0xFF;
        buf[3] = value & 0xFF;
        return 4;
    }
    if (value <= 0xFFFFFFFF) {
        buf[0] = 251;
        buf[1] = (value >> 24) & 0xFF;
        buf[2] = (value >> 16) & 0xFF;
        buf[3] = (value >> 8) & 0xFF;
        buf[4] = value & 0xFF;
        return 5;
    }
    if (value <= 0xFFFFFFFFFF) {
        buf[0] = 252;
        buf[1] = (value >> 32) & 0xFF;
        buf[2] = (value >> 24) & 0xFF;
        buf[3] = (value >> 16) & 0xFF;
        buf[4] = (value >> 8) & 0xFF;
        buf[5] = value & 0xFF;
        return 6;
    }
    if (value <= 0xFFFFFFFFFFFF) {
        buf[0] = 253;
        buf[1] = (value >> 40) & 0xFF;
        buf[2] = (value >> 32) & 0xFF;
        buf[3] = (value >> 24) & 0xFF;
        buf[4] = (value >> 16) & 0xFF;
        buf[5] = (value >> 8) & 0xFF;
        buf[6] = value & 0xFF;
        return 7;
    }
    if (value <= 0xFFFFFFFFFFFFFF) {
        buf[0] = 254;
        buf[1] = (value >> 48) & 0xFF;
        buf[2] = (value >> 40) & 0xFF;
        buf[3] = (value >> 32) & 0xFF;
        buf[4] = (value >> 24) & 0xFF;
        buf[5] = (value >> 16) & 0xFF;
        buf[6] = (value >> 8) & 0xFF;
        buf[7] = value & 0xFF;
        return 8;
    }
    // Full 8-byte value
    buf[0] = 255;
    buf[1] = (value >> 56) & 0xFF;
    buf[2] = (value >> 48) & 0xFF;
    buf[3] = (value >> 40) & 0xFF;
    buf[4] = (value >> 32) & 0xFF;
    buf[5] = (value >> 24) & 0xFF;
    buf[6] = (value >> 16) & 0xFF;
    buf[7] = (value >> 8) & 0xFF;
    buf[8] = value & 0xFF;
    return 9;
}

static inline uint32_t sqlite4_decode(const uint8_t *buf, uint64_t *value) {
    uint8_t first = buf[0];
    
    if (first <= 240) {
        *value = first;
        return 1;
    }
    if (first <= 248) {
        *value = 240 + 256 * (first - 241) + buf[1];
        return 2;
    }
    if (first == 249) {
        *value = 2288 + 256 * buf[1] + buf[2];
        return 3;
    }
    if (first == 250) {
        *value = ((uint64_t)buf[1] << 16) | 
                 ((uint64_t)buf[2] << 8) | 
                 buf[3];
        return 4;
    }
    if (first == 251) {
        *value = ((uint64_t)buf[1] << 24) | 
                 ((uint64_t)buf[2] << 16) | 
                 ((uint64_t)buf[3] << 8) | 
                 buf[4];
        return 5;
    }
    if (first == 252) {
        *value = ((uint64_t)buf[1] << 32) | 
                 ((uint64_t)buf[2] << 24) | 
                 ((uint64_t)buf[3] << 16) | 
                 ((uint64_t)buf[4] << 8) | 
                 buf[5];
        return 6;
    }
    if (first == 253) {
        *value = ((uint64_t)buf[1] << 40) | 
                 ((uint64_t)buf[2] << 32) | 
                 ((uint64_t)buf[3] << 24) | 
                 ((uint64_t)buf[4] << 16) | 
                 ((uint64_t)buf[5] << 8) | 
                 buf[6];
        return 7;
    }
    if (first == 254) {
        *value = ((uint64_t)buf[1] << 48) | 
                 ((uint64_t)buf[2] << 40) | 
                 ((uint64_t)buf[3] << 32) | 
                 ((uint64_t)buf[4] << 24) | 
                 ((uint64_t)buf[5] << 16) | 
                 ((uint64_t)buf[6] << 8) | 
                 buf[7];
        return 8;
    }
    // first == 255
    *value = ((uint64_t)buf[1] << 56) | 
             ((uint64_t)buf[2] << 48) | 
             ((uint64_t)buf[3] << 40) | 
             ((uint64_t)buf[4] << 32) | 
             ((uint64_t)buf[5] << 24) | 
             ((uint64_t)buf[6] << 16) | 
             ((uint64_t)buf[7] << 8) | 
             buf[8];
    return 9;
}

// Or just use memcpy if alignment allows (fastest)
static inline uint32_t int32_encode_direct(uint32_t value, uint8_t *buf) {
    memcpy(buf, &value, 4);
    return 4;
}

static inline uint32_t int32_decode_direct(const uint8_t *buf, uint32_t *value) {
    memcpy(value, buf, 4);
    return 4;
}

// Get a buffer from pool or allocate from heap
static inline uint8_t* kmh_get_buffer(size_t needed_size) {
    // Try to get from pool first
    for (int i = 0; i < MAX_SERIALIZE_BUFFERS; i++) {
        int expected = 0;
        if (atomic_compare_exchange_weak(&kmh_buffer_pool[i].busy, &expected, 1)) {
            // Successfully acquired this buffer
            if (needed_size <= sizeof(kmh_buffer_pool[i].buffer)) {
                return kmh_buffer_pool[i].buffer;
            } else {
                // Buffer too small, release and continue
                atomic_store(&kmh_buffer_pool[i].busy, 0);
            }
        }
    }
    
    // No available pool buffer, allocate from heap
    uint8_t* heap_buf = malloc(needed_size + sizeof(int));
    if (!heap_buf) return NULL;
    
    // Mark as heap allocated
    *((int*)heap_buf) = 1;
    return heap_buf + sizeof(int);
}

// Free buffer back to pool or heap
static inline void kmh_free_buffer(uint8_t* buf) {
    if (!buf) return;
    
    // Check if it's from our pool
    for (int i = 0; i < MAX_SERIALIZE_BUFFERS; i++) {
        if (buf == kmh_buffer_pool[i].buffer) {
            atomic_store(&kmh_buffer_pool[i].busy, 0);
            return;
        }
    }
    
    // Must be heap allocated - check the header
    uint8_t* actual_ptr = buf - sizeof(int);
    if (*((int*)actual_ptr) == 1) {
        free(actual_ptr);
    }
}

// Fast serialize - direct struct dump with minimal header
static inline uint32_t kmh_serialize(const kvalue_minhash_t *kmh, uint8_t **out_buf) {
    // Calculate total size: struct + hash array
    uint32_t struct_size = sizeof(kvalue_minhash_t);
    uint32_t hash_size = kmh->count * sizeof(uint32_t);
    uint32_t total_size = struct_size + hash_size;
    
    uint8_t *buf = kmh_get_buffer(total_size);
    if (!buf) return 0;
    
    // Copy the struct directly
    memcpy(buf, kmh, struct_size);
    
    // Fix the hashes pointer in the copied struct to point to the buffer location
    kvalue_minhash_t *copied_kmh = (kvalue_minhash_t*)buf;
    copied_kmh->hashes = (uint32_t*)(buf + struct_size);
    
    // Copy the hash array
    if (kmh->count > 0) {
        memcpy(buf + struct_size, kmh->hashes, hash_size);
    }
    
    *out_buf = buf;
    return total_size;
}


// Serialize (thread-safe optimized format)
static inline uint32_t kmh_serialize_old(const kvalue_minhash_t *kmh, uint8_t **out_buf) {
    // Calculate size needed
    uint32_t header_size = 4 * sizeof(uint32_t); // k, count, space_size, seed
    uint32_t data_size = 0;
    
    if (kmh->count > 0) {
        data_size = kmh->count * 4; // 4 bytes per hash
    }
    
    uint32_t total_size = header_size + data_size;
    
    uint8_t *buf = kmh_get_buffer(total_size);
    if (!buf) return 0;
    
    uint32_t pos = 0;
    
    // Header
    memcpy(buf + pos, &kmh->k, sizeof(uint32_t)); pos += sizeof(uint32_t);
    memcpy(buf + pos, &kmh->count, sizeof(uint32_t)); pos += sizeof(uint32_t);
    memcpy(buf + pos, &kmh->space_size, sizeof(uint32_t)); pos += sizeof(uint32_t);
    memcpy(buf + pos, &kmh->seed, sizeof(uint32_t)); pos += sizeof(uint32_t);
    
    if (kmh->count > 0) {
        for (int32_t i = 0; i < kmh->count; i++) {
            pos += int32_encode_direct(kmh->hashes[i], buf + pos);
        }
    }
    
    *out_buf = buf;
    return pos;
}

// Fast deserialize - direct struct read
static inline kvalue_minhash_t* kmh_deserialize(const uint8_t *buf, uint32_t buf_size) {
    if (buf_size < sizeof(kvalue_minhash_t)) return NULL;
    
    const kvalue_minhash_t *serialized_kmh = (const kvalue_minhash_t*)buf;
    
    // Validate the data makes sense
    if (serialized_kmh->count > serialized_kmh->k || 
        serialized_kmh->k > MAX_K * 10 || // Reasonable upper bound
        buf_size < sizeof(kvalue_minhash_t) + serialized_kmh->count * sizeof(uint32_t)) {
        return NULL;
    }
    
    kvalue_minhash_t *kmh = kmh_init(serialized_kmh->k, serialized_kmh->space_size, serialized_kmh->seed);
    if (!kmh) return NULL;
    
    // Copy the metadata
    kmh->count = serialized_kmh->count;
    
    // Copy the hash array
    if (kmh->count > 0) {
        const uint32_t *serialized_hashes = (const uint32_t*)(buf + sizeof(kvalue_minhash_t));
        memcpy(kmh->hashes, serialized_hashes, kmh->count * sizeof(uint32_t));
    }
    
    return kmh;
}


// Deserialize
static inline kvalue_minhash_t* kmh_deserialize_old(const uint8_t *buf, uint32_t buf_size) {
    if (buf_size < 4 * sizeof(uint32_t)) return NULL;
    uint32_t pos = 0;
    uint32_t k, count, space_size, seed;
    
    // Header
    memcpy(&k, buf + pos, sizeof(uint32_t)); pos += sizeof(uint32_t);
    memcpy(&count, buf + pos, sizeof(uint32_t)); pos += sizeof(uint32_t);
    memcpy(&space_size, buf + pos, sizeof(uint32_t)); pos += sizeof(uint32_t);
    memcpy(&seed, buf + pos, sizeof(uint32_t)); pos += sizeof(uint32_t);

    kvalue_minhash_t *kmh = kmh_init(k, space_size, seed);
    if (!kmh) return NULL;
    
    kmh->count = count;
    // Decode delta-encoded hashes
    if (count > 0) {
        for (int32_t i = 0; i < count && pos < buf_size; i++) {
            uint32_t current;
            pos += int32_decode_direct(buf + pos, &current);
            kmh->hashes[i] = current;
        }
    }
    
    return kmh;
}

// Fast cardinality from serialized data (without full deserialization)
static inline double kmh_cardinality_from_serialized(const uint8_t *buf, uint32_t buf_size) {
    if (buf_size < 4 * sizeof(uint32_t)) return -1.0;
    
    uint32_t k, count, space_size;
    memcpy(&k, buf, sizeof(uint32_t));
    memcpy(&count, buf + sizeof(uint32_t), sizeof(uint32_t));
    memcpy(&space_size, buf + 2 * sizeof(uint32_t), sizeof(uint32_t));
    
    if (count == 0) return 0.0;
    if (count < k) {
        return (double) count;
    }
    
    // Need k-th hash - decode up to position k-1
    uint32_t pos = 4 * sizeof(uint32_t);
    //uint32_t current = space_size;
    uint32_t delta;
    int decoded = int32_decode_direct(buf + pos, &delta);
    if(decoded > 0){
      //current -= delta;
      //return (double)space_size * (k - 1) / (current + 1);
      return (double)space_size * (k - 1) / (delta + 1);
    }
    
    return -1.0; // Error
}

#endif // KVALUE_MINHASH_H
