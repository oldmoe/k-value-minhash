#include "kmh.h"
#include <stdio.h>
#include <time.h>
#include <assert.h>
#include <math.h>

#define BENCH(name, iterations, code) do { \
   clock_t start = clock(); \
   for(int i = 0; i < iterations; i++) { code; } \
   double ms = (double)(clock() - start) * 1000 / CLOCKS_PER_SEC; \
   printf("%-20s: %8.2f ms (%8.1f ops/sec)\n", name, ms, iterations * 1000.0 / ms); \
} while(0)

int main() {
   const int N = 1000000;
   const int K = 400;
   const int SPACE = 10000000;
   
   printf("KValue MinHash Benchmark (N=%d, K=%d)\n", N, K);
   printf("================================================\n");
   
   kvalue_minhash_t *kmh0;
   BENCH("Allocate", 10000, kmh0 = kmh_init(K, SPACE, 0); kmh_free(kmh0););
   // Init
   kvalue_minhash_t *kmh = kmh_init(K, SPACE, 0);
   kvalue_minhash_t *kmh2 = kmh_init(K, SPACE, 0);
   assert(kmh && kmh2);
   
   // Add benchmark
   //BENCH("Add", N, kmh_add(kmh, rand()));
   BENCH("Add", N, kmh_add(kmh, (N/2)+i));
   printf("cardinality kmh %f\n", kmh_cardinality(kmh));
   // Fill second hash for merge/distance tests
   //for(int i = 0; i < N/2; i++) kmh_add(kmh2, rand());
   for(int i = 0; i < N/2; i++) kmh_add(kmh2, i);
   printf("cardinality kmh2 %f\n", kmh_cardinality(kmh2));  
   
   // Operations benchmark
   BENCH("Cardinality", 100000, kmh_cardinality(kmh));
   BENCH("Distance", 10000, kmh_distance(kmh, kmh2));
   
   // Serialization benchmark
   uint8_t *buf;
   uint32_t size;
   BENCH("Serialize", 10000, { 
       size = kmh_serialize(kmh, &buf); 
       kmh_free_buffer(buf); // Use pooled buffer
   });
   //free(buf);
   printf("Serialized size: %u bytes (%.1fx compression)\n", 
          size, (float)(K * sizeof(uint32_t)) / size);
   
   BENCH("Deserialize", 10000, {
       kvalue_minhash_t *tmp = kmh_deserialize(buf, size);
       kmh_free(tmp);
   });

   //BENCH("Deserialize Zero Copy", 10000, {
   //    kvalue_minhash_t *tmp = kmh_deserialize_zerocopy(buf, size);
       //kmh_free_zerocopy(tmp);
   //});

   BENCH("Fast cardinality", 100000, kmh_cardinality_from_serialized(buf, size));
   
   // Merge benchmark (create fresh hashes to avoid realloc issues)
   kvalue_minhash_t *a = kmh_init(K, SPACE, 42);
   kvalue_minhash_t *b = kmh_init(K, SPACE, 42); 
   for(int i = 0; i < 10000; i++) { kmh_add(a, i); kmh_add(b, i + 5000); }
   
   BENCH("Merge", 10000, {
       kvalue_minhash_t *merged = kmh_merge(a, b);
       kmh_free(merged);
   });
   
   // Accuracy test
   printf("\nAccuracy Test:\n");
   printf("Actual elements: %d, Estimated: %.0f (error: %.1f%%)\n", 
          N, kmh_cardinality(kmh), 
          100.0 * fabs(kmh_cardinality(kmh) - N) / N);
   
   // Cleanup
   // free(buf);
   kmh_free(kmh); kmh_free(kmh2); kmh_free(a); kmh_free(b);
   return 0;
}
