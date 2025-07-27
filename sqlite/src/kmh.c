#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1

#include "../../src/kmh.h"
#include <assert.h>

// Default parameters
#define DEFAULT_K 400
#define DEFAULT_SPACE_SIZE 0xFFFFFFFF
#define DEFAULT_SEED 42

// Helper function to extract MinHash from blob
static kvalue_minhash_t* kmh_from_blob(sqlite3_value *val) {
    if (sqlite3_value_type(val) != SQLITE_BLOB) {
        return NULL;
    }
    
    int blob_size = sqlite3_value_bytes(val);
    const uint8_t *blob_data = sqlite3_value_blob(val);
    
    if (!blob_data || blob_size < 16) { // Minimum header size
        return NULL;
    }
    
    return kmh_deserialize(blob_data, blob_size);
}

// Helper function to convert MinHash to blob
static void kmh_to_blob(sqlite3_context *context, kvalue_minhash_t *kmh) {
    uint8_t *serialized_data;
    uint32_t size = kmh_serialize(kmh, &serialized_data);
    
    if (size > 0 && serialized_data) {
        sqlite3_result_blob(context, serialized_data, size, SQLITE_TRANSIENT);
        kmh_free_buffer(serialized_data);
    } else {
        sqlite3_result_null(context);
    }
}

// kmh_create(value1, value2, ..., valueN)
static void kmh_create_func(sqlite3_context *context, int argc, sqlite3_value **argv) {
    if (argc == 0) {
        sqlite3_result_null(context);
        return;
    }
    
    kvalue_minhash_t *kmh = kmh_init(DEFAULT_K, DEFAULT_SPACE_SIZE, DEFAULT_SEED);
    if (!kmh) {
        sqlite3_result_error_nomem(context);
        return;
    }
    
    // Add all values
    for (int i = 0; i < argc; i++) {
        if (sqlite3_value_type(argv[i]) == SQLITE_INTEGER) {
            uint32_t value = (uint32_t)sqlite3_value_int64(argv[i]);
            kmh_add(kmh, value);
        }
        // Gracefully ignore NULL and non-integer values
    }
    
    kmh_to_blob(context, kmh);
    kmh_free(kmh);
}

// kmh_add(kmh_blob, value)
static void kmh_add_func(sqlite3_context *context, int argc, sqlite3_value **argv) {
    if (argc != 2) {
        sqlite3_result_error(context, "kmh_add requires exactly 2 arguments", -1);
        return;
    }
    
    kvalue_minhash_t *kmh = kmh_from_blob(argv[0]);
    if (!kmh) {
        sqlite3_result_null(context);
        return;
    }
    
    // Add the value if it's an integer
    if (sqlite3_value_type(argv[1]) == SQLITE_INTEGER) {
        uint32_t value = (uint32_t)sqlite3_value_int64(argv[1]);
        kmh_add(kmh, value);
    }
    
    kmh_to_blob(context, kmh);
    kmh_free(kmh);
}

// kmh_merge(kmh1, kmh2)
static void kmh_merge_func(sqlite3_context *context, int argc, sqlite3_value **argv) {
    if (argc != 2) {
        sqlite3_result_error(context, "kmh_merge requires exactly 2 arguments", -1);
        return;
    }
    
    kvalue_minhash_t *kmh1 = kmh_from_blob(argv[0]);
    kvalue_minhash_t *kmh2 = kmh_from_blob(argv[1]);
    
    if (!kmh1 || !kmh2) {
        if (kmh1) kmh_free(kmh1);
        if (kmh2) kmh_free(kmh2);
        sqlite3_result_null(context);
        return;
    }
    
    kvalue_minhash_t *result = kmh_merge(kmh1, kmh2);
    
    kmh_free(kmh1);
    kmh_free(kmh2);
    
    if (result) {
        kmh_to_blob(context, result);
        kmh_free(result);
    } else {
        sqlite3_result_null(context);
    }
}

// kmh_cardinality(kmh)
static void kmh_cardinality_func(sqlite3_context *context, int argc, sqlite3_value **argv) {
    if (argc != 1) {
        sqlite3_result_error(context, "kmh_cardinality requires exactly 1 argument", -1);
        return;
    }
    
    kvalue_minhash_t *kmh = kmh_from_blob(argv[0]);
    if (!kmh) {
        sqlite3_result_null(context);
        return;
    }
    
    double cardinality = kmh_cardinality(kmh);
    sqlite3_result_double(context, cardinality);
    
    kmh_free(kmh);
}

// kmh_merge_cardinality(kmh1, kmh2)
static void kmh_merge_cardinality_func(sqlite3_context *context, int argc, sqlite3_value **argv) {
    if (argc != 2) {
        sqlite3_result_error(context, "kmh_merge_cardinality requires exactly 2 arguments", -1);
        return;
    }
    
    kvalue_minhash_t *kmh1 = kmh_from_blob(argv[0]);
    kvalue_minhash_t *kmh2 = kmh_from_blob(argv[1]);
    
    if (!kmh1 || !kmh2) {
        if (kmh1) kmh_free(kmh1);
        if (kmh2) kmh_free(kmh2);
        sqlite3_result_null(context);
        return;
    }
    
    kvalue_minhash_t *result = kmh_merge(kmh1, kmh2);
    
    kmh_free(kmh1);
    kmh_free(kmh2);
    
    if (result) {
        double cardinality = kmh_cardinality(result);
        sqlite3_result_double(context, cardinality);
        kmh_free(result);
    } else {
        sqlite3_result_null(context);
    }
}

// Aggregate function context
typedef struct {
    kvalue_minhash_t *kmh;
} kmh_agg_context;

// kmh_group_create aggregate
static void kmh_group_create_step(sqlite3_context *context, int argc, sqlite3_value **argv) {
    kmh_agg_context *agg_ctx = sqlite3_aggregate_context(context, sizeof(kmh_agg_context));
    
    if (!agg_ctx) {
        sqlite3_result_error_nomem(context);
        return;
    }
    
    // Initialize on first call
    if (!agg_ctx->kmh) {
        agg_ctx->kmh = kmh_init(DEFAULT_K, DEFAULT_SPACE_SIZE, DEFAULT_SEED);
        if (!agg_ctx->kmh) {
            sqlite3_result_error_nomem(context);
            return;
        }
    }
    
    // Add value if it's an integer
    if (argc > 0 && sqlite3_value_type(argv[0]) == SQLITE_INTEGER) {
        uint32_t value = (uint32_t)sqlite3_value_int64(argv[0]);
        kmh_add(agg_ctx->kmh, value);
    }
}

static void kmh_group_create_final(sqlite3_context *context) {
    kmh_agg_context *agg_ctx = sqlite3_aggregate_context(context, 0);
    
    if (!agg_ctx || !agg_ctx->kmh) {
        sqlite3_result_null(context);
        return;
    }
    
    kmh_to_blob(context, agg_ctx->kmh);
    kmh_free(agg_ctx->kmh);
}

// kmh_group_merge aggregate
static void kmh_group_merge_step(sqlite3_context *context, int argc, sqlite3_value **argv) {
    kmh_agg_context *agg_ctx = sqlite3_aggregate_context(context, sizeof(kmh_agg_context));
    
    if (!agg_ctx) {
        sqlite3_result_error_nomem(context);
        return;
    }
    
    if (argc > 0 && sqlite3_value_type(argv[0]) == SQLITE_BLOB) {
        kvalue_minhash_t *input_kmh = kmh_from_blob(argv[0]);
        if (input_kmh) {
            if (!agg_ctx->kmh) {
                // First MinHash becomes the base
                agg_ctx->kmh = input_kmh;
            } else {
                // Merge with existing
                kvalue_minhash_t *merged = kmh_merge(agg_ctx->kmh, input_kmh);
                kmh_free(agg_ctx->kmh);
                kmh_free(input_kmh);
                agg_ctx->kmh = merged;
            }
        }
    }
}

static void kmh_group_merge_final(sqlite3_context *context) {
    kmh_agg_context *agg_ctx = sqlite3_aggregate_context(context, 0);
    
    if (!agg_ctx || !agg_ctx->kmh) {
        sqlite3_result_null(context);
        return;
    }
    
    kmh_to_blob(context, agg_ctx->kmh);
    kmh_free(agg_ctx->kmh);
}

// kmh_group_merge_cardinality aggregate
static void kmh_group_merge_cardinality_final(sqlite3_context *context) {
    kmh_agg_context *agg_ctx = sqlite3_aggregate_context(context, 0);
    
    if (!agg_ctx || !agg_ctx->kmh) {
        sqlite3_result_null(context);
        return;
    }
    
    double cardinality = kmh_cardinality(agg_ctx->kmh);
    sqlite3_result_double(context, cardinality);
    kmh_free(agg_ctx->kmh);
}

// Extension entry point
#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_kmh_init(sqlite3 *db, char **pzErrMsg, const sqlite3_api_routines *pApi) {
    SQLITE_EXTENSION_INIT2(pApi);
    
    int rc = SQLITE_OK;
    
    // Register scalar functions
    rc = sqlite3_create_function(db, "kmh_create", -1, SQLITE_UTF8, NULL, kmh_create_func, NULL, NULL);
    if (rc != SQLITE_OK) return rc;
    
    rc = sqlite3_create_function(db, "kmh_add", 2, SQLITE_UTF8, NULL, kmh_add_func, NULL, NULL);
    if (rc != SQLITE_OK) return rc;
    
    rc = sqlite3_create_function(db, "kmh_merge", 2, SQLITE_UTF8, NULL, kmh_merge_func, NULL, NULL);
    if (rc != SQLITE_OK) return rc;
    
    rc = sqlite3_create_function(db, "kmh_cardinality", 1, SQLITE_UTF8, NULL, kmh_cardinality_func, NULL, NULL);
    if (rc != SQLITE_OK) return rc;
    
    rc = sqlite3_create_function(db, "kmh_merge_cardinality", 2, SQLITE_UTF8, NULL, kmh_merge_cardinality_func, NULL, NULL);
    if (rc != SQLITE_OK) return rc;
    
    // Register aggregate functions
    rc = sqlite3_create_function(db, "kmh_group_create", 1, SQLITE_UTF8, NULL, NULL, kmh_group_create_step, kmh_group_create_final);
    if (rc != SQLITE_OK) return rc;
    
    rc = sqlite3_create_function(db, "kmh_group_merge", 1, SQLITE_UTF8, NULL, NULL, kmh_group_merge_step, kmh_group_merge_final);
    if (rc != SQLITE_OK) return rc;
    
    rc = sqlite3_create_function(db, "kmh_group_merge_cardinality", 1, SQLITE_UTF8, NULL, NULL, kmh_group_merge_step, kmh_group_merge_cardinality_final);
    if (rc != SQLITE_OK) return rc;
    
    return SQLITE_OK;
}
