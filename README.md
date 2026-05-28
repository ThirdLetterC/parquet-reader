# parquet-arrow-ffi

A small Rust shared/static library that exposes Apache Parquet read/write operations to C through the Arrow C Data / C Stream ABI.

This crate is useful when you want Rust's `parquet` and `arrow` implementation behind a C-compatible API, without exposing Rust types across the ABI boundary.

## What this library provides

- Read a Parquet file and receive an `ArrowArrayStream` in C.
- Write a Parquet file from an `ArrowArrayStream` produced by C, C++, nanoarrow, Arrow C++, or another Arrow-compatible producer.
- Query basic Parquet file metadata.
- Get a debug-formatted Arrow schema string for a Parquet file.
- Retrieve thread-local error messages from failed FFI calls.

The API is intentionally columnar. It does **not** expose custom row structs. Use the Arrow C Data / C Stream structures to consume or produce batches.

## Repository layout

```text
.
├── Cargo.toml
├── lib.rs
├── parquet_arrow_ffi.h
└── README.md
```

`Cargo.toml` currently points the library target at the root-level `lib.rs`:

```toml
[lib]
name = "parquet_arrow_ffi"
path = "lib.rs"
crate-type = ["cdylib", "staticlib"]
```

You can move `lib.rs` to `src/lib.rs` later if you prefer the standard Cargo layout; update `Cargo.toml` accordingly.

## Requirements

- Rust toolchain with Cargo.
- A C compiler for your platform.
- A C-side Arrow C Data consumer/producer, such as:
  - nanoarrow,
  - Apache Arrow C++,
  - your own implementation of `ArrowArray`, `ArrowSchema`, and `ArrowArrayStream`.

The included header contains minimal Arrow C Data / C Stream declarations. If your project already includes Arrow's official C ABI header, define `PARQUET_ARROW_FFI_SKIP_ARROW_ABI` before including `parquet_arrow_ffi.h`.

## Build

```bash
cargo build --release
```

Typical output paths:

```text
Linux:   target/release/libparquet_arrow_ffi.so
macOS:   target/release/libparquet_arrow_ffi.dylib
Windows: target/release/parquet_arrow_ffi.dll
Static:  target/release/libparquet_arrow_ffi.a
```

Example Linux compile/link command for a C program:

```bash
cc main.c \
  -I. \
  -Ltarget/release \
  -lparquet_arrow_ffi \
  -o main
```

At runtime on Linux, make sure the shared library can be found:

```bash
export LD_LIBRARY_PATH="$PWD/target/release:$LD_LIBRARY_PATH"
./main
```

On macOS, use `DYLD_LIBRARY_PATH` or install the `.dylib` in a loader-visible location. On Windows, place the `.dll` next to your executable or in a directory on `PATH`.

## C API

Include:

```c
#include "parquet_arrow_ffi.h"
```

### Return codes

```c
#define PARQUET_ARROW_OK     0
#define PARQUET_ARROW_ERROR -1
#define PARQUET_ARROW_PANIC -2
```

- `PARQUET_ARROW_OK`: call succeeded.
- `PARQUET_ARROW_ERROR`: normal error; call `parquet_arrow_last_error_message()`.
- `PARQUET_ARROW_PANIC`: Rust panic was caught before crossing the C ABI boundary; call `parquet_arrow_last_error_message()`.

### Functions

```c
const char* parquet_arrow_last_error_message(void);
void parquet_arrow_clear_last_error(void);
void parquet_arrow_free_c_string(char* s);
void parquet_arrow_release_stream(struct ArrowArrayStream* stream);
```

```c
int parquet_arrow_file_info(
    const char* path,
    ParquetArrowFileInfo* out_info);
```

```c
int parquet_arrow_schema_string(
    const char* path,
    char** out_schema_text);
```

```c
int parquet_arrow_read_file(
    const char* path,
    size_t batch_size,
    struct ArrowArrayStream* out_stream);
```

```c
int parquet_arrow_write_file(
    const char* path,
    struct ArrowArrayStream* input_stream,
    int compression,
    size_t max_row_group_row_count);
```

## Compression constants

Use these values with `parquet_arrow_write_file()`:

```c
enum ParquetArrowCompression {
  PARQUET_ARROW_COMPRESSION_UNCOMPRESSED = 0,
  PARQUET_ARROW_COMPRESSION_SNAPPY = 1,
  PARQUET_ARROW_COMPRESSION_GZIP = 2,
  PARQUET_ARROW_COMPRESSION_BROTLI = 3,
  PARQUET_ARROW_COMPRESSION_ZSTD = 4,
  PARQUET_ARROW_COMPRESSION_LZ4_RAW = 5
};
```

## Ownership and memory rules

### Error strings

`parquet_arrow_last_error_message()` returns a pointer owned by Rust. Do **not** free it. The pointer remains valid until the next FFI call on the same thread that changes or clears the stored error.

```c
const char* msg = parquet_arrow_last_error_message();
```

### Schema strings

`parquet_arrow_schema_string()` allocates a C string. Free it with `parquet_arrow_free_c_string()`.

```c
char* schema = NULL;
int rc = parquet_arrow_schema_string("input.parquet", &schema);
if (rc == PARQUET_ARROW_OK) {
    puts(schema);
    parquet_arrow_free_c_string(schema);
}
```

### Streams returned by `parquet_arrow_read_file()`

`parquet_arrow_read_file()` writes a valid `ArrowArrayStream` into caller-provided storage. Release it when done:

```c
parquet_arrow_release_stream(&stream);
```

or:

```c
if (stream.release) {
    stream.release(&stream);
}
```

Every `ArrowSchema` and `ArrowArray` returned by the stream must also be released according to Arrow C Data rules.

### Streams passed to `parquet_arrow_write_file()`

`parquet_arrow_write_file()` imports and consumes `input_stream`. After calling it, do not use or release that stream from C.

## Example: error handling helper

```c
#include <stdio.h>
#include <stdlib.h>
#include "parquet_arrow_ffi.h"

static void check_parquet_arrow(int rc) {
    if (rc != PARQUET_ARROW_OK) {
        const char* msg = parquet_arrow_last_error_message();
        fprintf(stderr, "parquet_arrow_ffi error: %s\n", msg ? msg : "unknown error");
        exit(1);
    }
}
```

## Example: print file info and schema

```c
#include <stdio.h>
#include <stdlib.h>
#include "parquet_arrow_ffi.h"

static void check_parquet_arrow(int rc) {
    if (rc != PARQUET_ARROW_OK) {
        const char* msg = parquet_arrow_last_error_message();
        fprintf(stderr, "parquet_arrow_ffi error: %s\n", msg ? msg : "unknown error");
        exit(1);
    }
}

int main(void) {
    ParquetArrowFileInfo info;
    check_parquet_arrow(parquet_arrow_file_info("input.parquet", &info));

    printf("rows:       %lld\n", (long long)info.num_rows);
    printf("row groups: %zu\n", info.num_row_groups);
    printf("columns:    %zu\n", info.num_columns);

    char* schema = NULL;
    check_parquet_arrow(parquet_arrow_schema_string("input.parquet", &schema));
    printf("schema:\n%s\n", schema);
    parquet_arrow_free_c_string(schema);

    return 0;
}
```

## Example: read a Parquet file as Arrow batches

This example opens a Parquet file and iterates over Arrow record batches. It only demonstrates stream handling; actual column decoding should be done with nanoarrow, Arrow C++, or your own Arrow C Data consumer.

```c
#include <stdio.h>
#include <stdlib.h>
#include "parquet_arrow_ffi.h"

static void check_parquet_arrow(int rc) {
    if (rc != PARQUET_ARROW_OK) {
        const char* msg = parquet_arrow_last_error_message();
        fprintf(stderr, "parquet_arrow_ffi error: %s\n", msg ? msg : "unknown error");
        exit(1);
    }
}

int main(void) {
    struct ArrowArrayStream stream = {0};

    /* batch_size = 65536 rows. Pass 0 to use the crate default. */
    check_parquet_arrow(parquet_arrow_read_file("input.parquet", 65536, &stream));

    struct ArrowSchema schema = {0};
    check_parquet_arrow(stream.get_schema(&stream, &schema));

    long long batch_count = 0;
    long long row_count = 0;

    while (1) {
        struct ArrowArray batch = {0};
        check_parquet_arrow(stream.get_next(&stream, &batch));

        /* End of stream is represented by release == NULL. */
        if (batch.release == NULL) {
            break;
        }

        batch_count += 1;
        row_count += batch.length;

        /* Consume batch here. Then release it. */
        batch.release(&batch);
    }

    printf("batches: %lld\n", batch_count);
    printf("rows:    %lld\n", row_count);

    if (schema.release) {
        schema.release(&schema);
    }

    parquet_arrow_release_stream(&stream);
    return 0;
}
```

## Example: write a Parquet file from C

Your C code must provide an `ArrowArrayStream`. This can come from nanoarrow, Arrow C++, or another producer implementing the Arrow C Stream Interface.

```c
#include <stdio.h>
#include <stdlib.h>
#include "parquet_arrow_ffi.h"

static void check_parquet_arrow(int rc) {
    if (rc != PARQUET_ARROW_OK) {
        const char* msg = parquet_arrow_last_error_message();
        fprintf(stderr, "parquet_arrow_ffi error: %s\n", msg ? msg : "unknown error");
        exit(1);
    }
}

int main(void) {
    struct ArrowArrayStream input_stream = {0};

    /*
       Fill input_stream from your C-side Arrow producer.
       For example: nanoarrow, Arrow C++, or a custom ArrowArrayStream.
    */

    check_parquet_arrow(parquet_arrow_write_file(
        "output.parquet",
        &input_stream,
        PARQUET_ARROW_COMPRESSION_ZSTD,
        0));

    /*
       input_stream has been consumed by parquet_arrow_write_file().
       Do not use it or release it here.
    */

    return 0;
}
```

## Path encoding

All paths passed to this library must be UTF-8, NUL-terminated C strings.

## Threading notes

The last-error message is stored per thread. A failure on one thread does not overwrite the last-error message on another thread.

## Safety notes

- Do not pass null pointers unless the function explicitly allows it.
- Do not let Rust-owned pointers escape beyond their documented lifetime.
- Always release `ArrowSchema` and `ArrowArray` values returned through `ArrowArrayStream`.
- Release streams returned by `parquet_arrow_read_file()` exactly once.
- Treat streams passed to `parquet_arrow_write_file()` as consumed, even when the call fails after importing the stream.

## Development

Format Rust code:

```bash
cargo fmt
```

Check the Rust crate:

```bash
cargo check
```

Build release libraries:

```bash
cargo build --release
```

## Suggested next improvements

- Add integration tests that read a known Parquet file from C.
- Add a nanoarrow-based C example that creates an `ArrowArrayStream` and writes it to Parquet.
- Generate the C header with `cbindgen` if the Rust API grows.
- Add CI for Linux, macOS, and Windows release builds.
- Add a license file before publishing the repository.
