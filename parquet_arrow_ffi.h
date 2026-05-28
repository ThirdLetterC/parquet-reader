#ifndef PARQUET_ARROW_FFI_H
#define PARQUET_ARROW_FFI_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Minimal Arrow C Data Interface declarations.
   If your project already includes arrow/c/abi.h, include that instead and define
   PARQUET_ARROW_FFI_SKIP_ARROW_ABI before including this header. */
#ifndef PARQUET_ARROW_FFI_SKIP_ARROW_ABI

#ifndef ARROW_C_DATA_INTERFACE
#define ARROW_C_DATA_INTERFACE

#define ARROW_FLAG_DICTIONARY_ORDERED 1
#define ARROW_FLAG_NULLABLE 2
#define ARROW_FLAG_MAP_KEYS_SORTED 4

struct ArrowSchema {
  const char* format;
  const char* name;
  const char* metadata;
  int64_t flags;
  int64_t n_children;
  struct ArrowSchema** children;
  struct ArrowSchema* dictionary;
  void (*release)(struct ArrowSchema*);
  void* private_data;
};

struct ArrowArray {
  int64_t length;
  int64_t null_count;
  int64_t offset;
  int64_t n_buffers;
  int64_t n_children;
  const void** buffers;
  struct ArrowArray** children;
  struct ArrowArray* dictionary;
  void (*release)(struct ArrowArray*);
  void* private_data;
};

#endif /* ARROW_C_DATA_INTERFACE */

#ifndef ARROW_C_STREAM_INTERFACE
#define ARROW_C_STREAM_INTERFACE

struct ArrowArrayStream {
  int (*get_schema)(struct ArrowArrayStream*, struct ArrowSchema* out);
  int (*get_next)(struct ArrowArrayStream*, struct ArrowArray* out);
  const char* (*get_last_error)(struct ArrowArrayStream*);
  void (*release)(struct ArrowArrayStream*);
  void* private_data;
};

#endif /* ARROW_C_STREAM_INTERFACE */
#endif /* PARQUET_ARROW_FFI_SKIP_ARROW_ABI */

#define PARQUET_ARROW_OK 0
#define PARQUET_ARROW_ERROR -1
#define PARQUET_ARROW_PANIC -2

enum ParquetArrowCompression {
  PARQUET_ARROW_COMPRESSION_UNCOMPRESSED = 0,
  PARQUET_ARROW_COMPRESSION_SNAPPY = 1,
  PARQUET_ARROW_COMPRESSION_GZIP = 2,
  PARQUET_ARROW_COMPRESSION_BROTLI = 3,
  PARQUET_ARROW_COMPRESSION_ZSTD = 4,
  PARQUET_ARROW_COMPRESSION_LZ4_RAW = 5
};

typedef struct ParquetArrowFileInfo {
  int64_t num_rows;
  size_t num_row_groups;
  size_t num_columns;
} ParquetArrowFileInfo;

const char* parquet_arrow_last_error_message(void);
void parquet_arrow_clear_last_error(void);
void parquet_arrow_free_c_string(char* s);
void parquet_arrow_release_stream(struct ArrowArrayStream* stream);

int parquet_arrow_file_info(
    const char* path,
    ParquetArrowFileInfo* out_info);

int parquet_arrow_schema_string(
    const char* path,
    char** out_schema_text);

int parquet_arrow_read_file(
    const char* path,
    size_t batch_size,
    struct ArrowArrayStream* out_stream);

int parquet_arrow_write_file(
    const char* path,
    struct ArrowArrayStream* input_stream,
    int compression,
    size_t max_row_group_row_count);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* PARQUET_ARROW_FFI_H */
