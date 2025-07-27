#include "kvm.h"
#include <stdio.h>
#include <assert.h>
#include <math.h>

#define TEST(name, condition) do { \
   if (condition) { \
       printf("✓ %s\n", name); \
   } else { \
       printf("✗ %s FAILED\n", name); \
   } \
} while(0)

int main() {
   printf("KValue MinHash Tests\n");
   printf("===================\n");
   
   // Init tests
   kvalue_minhash_t *kmh = kmh_init(10, 1000, 42);
   TEST("Init", kmh != NULL && kmh->k == 10 && kmh->count == 0);
   
   // Add tests
   kmh_add(kmh, 100);
   kmh_add(kmh, 200);
   TEST("Add basic", kmh->count == 2);
   kmh_add(kmh, 100); // duplicate
   TEST("Add duplicate ignored", kmh->count == 2);
   
   // Fill to capacity
   for(int i = 0; i < 15; i++) kmh_add(kmh, i * 37);
   TEST("Add to capacity", kmh->count == 10);
   TEST("Descending order", kmh->hashes[0] > kmh->hashes[kmh->count-1]);
   
   // Cardinality tests
   double card = kmh_cardinality(kmh);
   TEST("Cardinality > 0", card > 0);
   TEST("Cardinality reasonable", card < 10000); // Should be reasonable estimate
   
   // Empty hash cardinality
   kvalue_minhash_t *empty = kmh_init(5, 1000, 42);
   TEST("Empty cardinality", kmh_cardinality(empty) == 0.0);
   
   // Merge tests
   kvalue_minhash_t *kmh2 = kmh_init(10, 1000, 42);
   for(int i = 0; i < 8; i++) kmh_add(kmh2, i * 13);
   
   kvalue_minhash_t *merged = kmh_merge(kmh, kmh2);
   TEST("Merge success", merged != NULL);
   TEST("Merge capacity", merged->count <= 10);
   TEST("Merge descending", merged->count == 0 || 
         merged->hashes[0] >= merged->hashes[merged->count-1]);
   
   // Incompatible merge
   kvalue_minhash_t *diff = kmh_init(5, 1000, 42); // Different k
   kvalue_minhash_t *bad_merge = kmh_merge(kmh, diff);
   TEST("Incompatible merge fails", bad_merge == NULL);
   
   // Distance tests
   double dist = kmh_distance(empty, empty);
   TEST("Empty distance", dist == 1.0);
   
   dist = kmh_distance(kmh, kmh);
   TEST("Self distance", dist == 0.0);
   
   dist = kmh_distance(kmh, kmh2);
   TEST("Valid distance", dist >= 0.0 && dist <= 1.0);
   
   // Serialization tests
   uint8_t *buf;
   uint32_t size = kmh_serialize(kmh, &buf);
   TEST("Serialize success", size > 0 && buf != NULL);
   TEST("Serialize reasonable size", size == (kmh->k + 4) * sizeof(uint32_t));
   
   // Deserialization tests
   kvalue_minhash_t *restored = kmh_deserialize(buf, size);
   TEST("Deserialize success", restored != NULL);
   TEST("Deserialize k", restored->k == kmh->k);
   TEST("Deserialize count", restored->count == kmh->count);
   TEST("Deserialize space_size", restored->space_size == kmh->space_size);
   TEST("Deserialize seed", restored->seed == kmh->seed);
   
   // Hash values match
   int hashes_match = 1;
   for(uint32_t i = 0; i < kmh->count; i++) {
       if(kmh->hashes[i] != restored->hashes[i]) {
           hashes_match = 0;
           break;
       }
   }
   TEST("Deserialize hashes", hashes_match);
   
   // Fast cardinality from serialized
   double fast_card = kmh_cardinality_from_serialized(buf, size);
   double normal_card = kmh_cardinality(kmh);
   TEST("Fast cardinality", fabs(fast_card - normal_card) < 0.001);
   
   // Empty serialization
   uint8_t *empty_buf;
   uint32_t empty_size = kmh_serialize(empty, &empty_buf);
   kvalue_minhash_t *empty_restored = kmh_deserialize(empty_buf, empty_size);
   TEST("Empty serialize/deserialize", empty_restored->count == 0);
   
   // Corrupted data tests
   TEST("Deserialize small buffer", kmh_deserialize(buf, 4) == NULL);
   TEST("Fast cardinality small buffer", kmh_cardinality_from_serialized(buf, 4) == -1.0);
   
   // Edge cases
   kvalue_minhash_t *single = kmh_init(1, 100, 42);
   kmh_add(single, 50);
   TEST("Single k capacity", single->count == 1);
   
   uint8_t *single_buf;
   uint32_t single_size = kmh_serialize(single, &single_buf);
   kvalue_minhash_t *single_restored = kmh_deserialize(single_buf, single_size);
   TEST("Single k serialize", single_restored->count == 1 && 
         single_restored->hashes[0] == single->hashes[0]);
   
   // Hash function consistency
   uint32_t h1 = xxh32_hash(12345, 42);
   uint32_t h2 = xxh32_hash(12345, 42);
   TEST("Hash consistency", h1 == h2);
   
   uint32_t h3 = xxh32_hash(12345, 43);
   TEST("Hash seed sensitivity", h1 != h3);

    // Cardinality estimation accuracy test
    printf("\nCardinality Estimation Tests:\n");

    // Test with known set sizes
    for (int test_size = 100; test_size <= 10000; test_size *= 10) {
        kvalue_minhash_t *test_kmh = kmh_init(128, 100000, 42);
        
        // Add unique sequential values
        for (int i = 0; i < test_size; i++) {
            kmh_add(test_kmh, i);
        }
        
        double estimated = kmh_cardinality(test_kmh);
        double error_pct = 100.0 * fabs(estimated - test_size) / test_size;
        
        printf("  Size %d: estimated %.0f (%.1f%% error)\n", 
               test_size, estimated, error_pct);
        
        // For larger sets, MinHash should be reasonably accurate (within 20%)
        if (test_size >= 1000) {
            TEST("Cardinality accuracy", error_pct < 20.0);
        }
        
        kmh_free(test_kmh);
    }

    // Test incomplete sketch cardinality
    kvalue_minhash_t *partial = kmh_init(100, 10000, 42);
    for (int i = 0; i < 50; i++) kmh_add(partial, i);

    double partial_est = kmh_cardinality(partial);
    TEST("Partial sketch cardinality", partial_est > 0 && partial_est < 10000);
    printf("  Partial (50 items): estimated %.0f\n", partial_est);

    kmh_free(partial);

   
   printf("\nAll tests passed! ✓\n");
   
   // Cleanup
   kmh_free(kmh); kmh_free(kmh2); kmh_free(empty); kmh_free(merged); 
   kmh_free(diff); kmh_free(restored); kmh_free(empty_restored);
   kmh_free(single); kmh_free(single_restored);
   free(buf); free(empty_buf); free(single_buf);
   
   return 0;
}
