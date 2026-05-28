/*
   Example C program for parquet_arrow_ffi.

   It opens a Parquet file through the Rust Parquet/Arrow shared library,
   reads Arrow record batches through ArrowArrayStream, and prints sample rows
   directly from the Arrow C Data buffers.

   This version is Windows-console friendly: UTF-8 string columns are decoded
   and written with WriteConsoleW when stdout is a Windows console. This avoids
   mojibake for Cyrillic / Ukrainian text in the `text` column.

   Build with MinGW GNU after building the Rust DLL:

     cargo build --release --target x86_64-pc-windows-gnu

     gcc sample_print_data.c -I. \
       target/x86_64-pc-windows-gnu/release/libparquet_arrow_ffi.dll.a \
       -o sample_print_data.exe

     copy target\\x86_64-pc-windows-gnu\\release\\parquet_arrow_ffi.dll .
     ./sample_print_data.exe input.parquet 5

   Arguments:
     argv[1] = parquet path, default: input.parquet
     argv[2] = number of rows to print, default: 5
     argv[3] = batch size, default: 65536
     argv[4] = max source bytes scanned for string/binary values, default: 220
     argv[5] = text mode: clean, utf8, or ascii; default: clean

   Modes:
     clean = print valid UTF-8, but escape invalid bytes, controls, bidi marks,
             private-use chars, and other terminal-hostile characters
     utf8  = print valid UTF-8 directly and only escape basic controls
     ascii = ASCII-only output; every non-ASCII code point becomes \uXXXX/\UXXXXXXXX

   Examples:
     sample_print_data.exe input.parquet 5
     sample_print_data.exe input.parquet 3 65536 800 clean
     sample_print_data.exe input.parquet 3 65536 800 ascii
*/

#include <errno.h>
#include <inttypes.h>
#include <locale.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#endif

#include "parquet_arrow_ffi.h"

#define DEFAULT_SAMPLE_ROWS 5u
#define DEFAULT_BATCH_SIZE 65536u
#define DEFAULT_STRING_LIMIT_BYTES 220u

typedef enum TextPrintMode {
    TEXT_PRINT_CLEAN = 0,
    TEXT_PRINT_UTF8 = 1,
    TEXT_PRINT_ASCII_ESCAPES = 2
} TextPrintMode;

static void configure_process_for_utf8_output(void) {
    setlocale(LC_ALL, "");
#ifdef _WIN32
    /* This helps printf/fwrite UTF-8 output in Windows terminals. The string
       printer below also uses WriteConsoleW directly when possible. */
    SetConsoleOutputCP(CP_UTF8);
    SetConsoleCP(CP_UTF8);
#endif
}

static void check_api(int rc) {
    if (rc != PARQUET_ARROW_OK) {
        const char* msg = parquet_arrow_last_error_message();
        fprintf(stderr, "parquet_arrow_ffi error: %s\n", msg ? msg : "(no message)");
        exit(1);
    }
}

static void check_stream(int rc, struct ArrowArrayStream* stream) {
    if (rc != 0) {
        const char* msg = NULL;
        if (stream != NULL && stream->get_last_error != NULL) {
            msg = stream->get_last_error(stream);
        }
        fprintf(stderr, "ArrowArrayStream error: %s\n", msg ? msg : "(no stream error message)");
        exit(1);
    }
}

static size_t parse_size_arg(const char* text, size_t default_value) {
    char* end = NULL;
    unsigned long long value;

    if (text == NULL || text[0] == '\0') {
        return default_value;
    }

    errno = 0;
    value = strtoull(text, &end, 10);
    if (errno != 0 || end == text || *end != '\0') {
        fprintf(stderr, "warning: invalid number '%s'; using %zu\n", text, default_value);
        return default_value;
    }

    return (size_t)value;
}

static TextPrintMode parse_text_mode(const char* text) {
    if (text == NULL || text[0] == '\0') {
        return TEXT_PRINT_CLEAN;
    }
    if (strcmp(text, "clean") == 0 || strcmp(text, "safe") == 0) {
        return TEXT_PRINT_CLEAN;
    }
    if (strcmp(text, "ascii") == 0 ||
        strcmp(text, "escape") == 0 ||
        strcmp(text, "escaped") == 0 ||
        strcmp(text, "ascii-escape") == 0 ||
        strcmp(text, "ascii_escapes") == 0) {
        return TEXT_PRINT_ASCII_ESCAPES;
    }
    if (strcmp(text, "utf8") == 0 || strcmp(text, "utf-8") == 0 || strcmp(text, "unicode") == 0) {
        return TEXT_PRINT_UTF8;
    }
    fprintf(stderr, "warning: unknown text mode '%s'; using clean\n", text);
    return TEXT_PRINT_CLEAN;
}

static const char* text_mode_name(TextPrintMode mode) {
    if (mode == TEXT_PRINT_ASCII_ESCAPES) {
        return "ascii";
    }
    if (mode == TEXT_PRINT_UTF8) {
        return "utf8";
    }
    return "clean";
}

static const char* safe_format(const struct ArrowSchema* schema) {
    return (schema != NULL && schema->format != NULL) ? schema->format : "";
}

static const char* safe_name(const struct ArrowSchema* schema, int64_t column_index) {
    if (schema == NULL || schema->children == NULL) {
        return "(field)";
    }
    if (column_index < 0 || column_index >= schema->n_children) {
        return "(field)";
    }
    if (schema->children[column_index] == NULL || schema->children[column_index]->name == NULL) {
        return "(field)";
    }
    if (schema->children[column_index]->name[0] == '\0') {
        return "(field)";
    }
    return schema->children[column_index]->name;
}

static int arrow_is_valid(const struct ArrowArray* array, int64_t logical_index) {
    const uint8_t* validity;
    int64_t bit_index;

    if (array == NULL) {
        return 0;
    }

    if (array->null_count == 0 || array->buffers == NULL || array->buffers[0] == NULL) {
        return 1;
    }

    validity = (const uint8_t*)array->buffers[0];
    bit_index = array->offset + logical_index;
    if (bit_index < 0) {
        return 0;
    }

    return ((validity[bit_index >> 3] >> (bit_index & 7)) & 1u) != 0;
}

static int bitmap_value(const uint8_t* bitmap, int64_t bit_index) {
    if (bitmap == NULL || bit_index < 0) {
        return 0;
    }
    return ((bitmap[bit_index >> 3] >> (bit_index & 7)) & 1u) != 0;
}

static int64_t value_index(const struct ArrowArray* array, int64_t logical_index) {
    return array->offset + logical_index;
}

#ifdef _WIN32
static void write_utf8_bytes_to_stdout(const char* data, size_t len) {
    HANDLE out;
    DWORD mode;

    if (data == NULL || len == 0) {
        return;
    }

    out = GetStdHandle(STD_OUTPUT_HANDLE);
    if (out != NULL && out != INVALID_HANDLE_VALUE && GetConsoleMode(out, &mode)) {
        fflush(stdout);
        while (len > 0) {
            int chunk_len;
            int wide_len;
            WCHAR* wide;
            DWORD written = 0;

            chunk_len = len > 32768u ? 32768 : (int)len;
            wide_len = MultiByteToWideChar(CP_UTF8,
                                           MB_ERR_INVALID_CHARS,
                                           data,
                                           chunk_len,
                                           NULL,
                                           0);
            if (wide_len <= 0) {
                break;
            }

            wide = (WCHAR*)malloc((size_t)wide_len * sizeof(WCHAR));
            if (wide == NULL) {
                break;
            }

            wide_len = MultiByteToWideChar(CP_UTF8,
                                           MB_ERR_INVALID_CHARS,
                                           data,
                                           chunk_len,
                                           wide,
                                           wide_len);
            if (wide_len > 0) {
                WriteConsoleW(out, wide, (DWORD)wide_len, &written, NULL);
            }
            free(wide);

            data += chunk_len;
            len -= (size_t)chunk_len;
        }

        if (len == 0) {
            return;
        }
    }

    fwrite(data, 1, len, stdout);
}
#else
static void write_utf8_bytes_to_stdout(const char* data, size_t len) {
    if (data != NULL && len > 0) {
        fwrite(data, 1, len, stdout);
    }
}
#endif

static int is_utf8_continuation(unsigned char c) {
    return (c & 0xC0u) == 0x80u;
}

static int utf8_decode_one(const unsigned char* s,
                           int64_t available,
                           uint32_t* out_codepoint,
                           int* out_consumed) {
    unsigned char b0;
    uint32_t cp;

    if (s == NULL || available <= 0 || out_codepoint == NULL || out_consumed == NULL) {
        return 0;
    }

    b0 = s[0];
    if (b0 < 0x80u) {
        *out_codepoint = (uint32_t)b0;
        *out_consumed = 1;
        return 1;
    }

    /* 2-byte sequence: U+0080..U+07FF. Exclude overlong C0/C1. */
    if (b0 >= 0xC2u && b0 <= 0xDFu) {
        if (available < 2 || !is_utf8_continuation(s[1])) {
            return 0;
        }
        cp = ((uint32_t)(b0 & 0x1Fu) << 6) |
             (uint32_t)(s[1] & 0x3Fu);
        *out_codepoint = cp;
        *out_consumed = 2;
        return 1;
    }

    /* 3-byte sequence. Exclude overlongs and UTF-16 surrogate range. */
    if (b0 >= 0xE0u && b0 <= 0xEFu) {
        if (available < 3 || !is_utf8_continuation(s[1]) || !is_utf8_continuation(s[2])) {
            return 0;
        }
        if (b0 == 0xE0u && s[1] < 0xA0u) {
            return 0;
        }
        if (b0 == 0xEDu && s[1] >= 0xA0u) {
            return 0;
        }
        cp = ((uint32_t)(b0 & 0x0Fu) << 12) |
             ((uint32_t)(s[1] & 0x3Fu) << 6) |
             (uint32_t)(s[2] & 0x3Fu);
        *out_codepoint = cp;
        *out_consumed = 3;
        return 1;
    }

    /* 4-byte sequence: U+010000..U+10FFFF. */
    if (b0 >= 0xF0u && b0 <= 0xF4u) {
        if (available < 4 ||
            !is_utf8_continuation(s[1]) ||
            !is_utf8_continuation(s[2]) ||
            !is_utf8_continuation(s[3])) {
            return 0;
        }
        if (b0 == 0xF0u && s[1] < 0x90u) {
            return 0;
        }
        if (b0 == 0xF4u && s[1] > 0x8Fu) {
            return 0;
        }
        cp = ((uint32_t)(b0 & 0x07u) << 18) |
             ((uint32_t)(s[1] & 0x3Fu) << 12) |
             ((uint32_t)(s[2] & 0x3Fu) << 6) |
             (uint32_t)(s[3] & 0x3Fu);
        *out_codepoint = cp;
        *out_consumed = 4;
        return 1;
    }

    return 0;
}

static void print_unicode_escape(uint32_t cp) {
    if (cp <= 0xFFFFu) {
        printf("\\u%04X", (unsigned)cp);
    } else {
        printf("\\U%08X", (unsigned)cp);
    }
}

static int is_terminal_hostile_codepoint(uint32_t cp) {
    if (cp <= 0x1Fu || cp == 0x7Fu) {
        return 1;
    }
    if (cp >= 0x80u && cp <= 0x9Fu) {
        return 1;
    }
    if (cp >= 0x200Bu && cp <= 0x200Fu) {
        return 1;
    }
    if (cp >= 0x202Au && cp <= 0x202Eu) {
        return 1;
    }
    if (cp >= 0x2060u && cp <= 0x206Fu) {
        return 1;
    }
    if (cp == 0xFEFFu || cp == 0xFFFDu) {
        return 1;
    }
    if ((cp >= 0xE000u && cp <= 0xF8FFu) ||
        (cp >= 0xF0000u && cp <= 0xFFFFDu) ||
        (cp >= 0x100000u && cp <= 0x10FFFDu)) {
        return 1;
    }
    if (cp >= 0xFFF0u && cp <= 0xFFFFu) {
        return 1;
    }
    return 0;
}

static void print_escaped_utf8(const char* data,
                               int64_t byte_length,
                               size_t max_bytes,
                               TextPrintMode text_mode) {
    int64_t hard_limit;
    int64_t pos = 0;

    if (data == NULL && byte_length > 0) {
        printf("<null-data>");
        return;
    }

    if (byte_length < 0) {
        printf("<bad-length>");
        return;
    }

    hard_limit = byte_length;
    if ((uint64_t)hard_limit > (uint64_t)max_bytes) {
        hard_limit = (int64_t)max_bytes;
    }

    putchar('"');

    while (pos < hard_limit) {
        const unsigned char* p = (const unsigned char*)data + pos;
        unsigned char c = *p;
        uint32_t cp = 0;
        int consumed = 0;

        if (c < 0x80u) {
            if (c == '\\') {
                fputs("\\\\", stdout);
            } else if (c == '"') {
                fputs("\\\"", stdout);
            } else if (c == '\n') {
                fputs("\\n", stdout);
            } else if (c == '\r') {
                fputs("\\r", stdout);
            } else if (c == '\t') {
                fputs("\\t", stdout);
            } else if (c < 0x20u || c == 0x7Fu) {
                printf("\\x%02X", (unsigned)c);
            } else {
                putchar((int)c);
            }
            ++pos;
            continue;
        }

        if (!utf8_decode_one(p, byte_length - pos, &cp, &consumed)) {
            printf("\\x%02X", (unsigned)c);
            ++pos;
            continue;
        }

        if (pos + consumed > hard_limit) {
            break;
        }

        if (text_mode == TEXT_PRINT_ASCII_ESCAPES) {
            print_unicode_escape(cp);
        } else if (text_mode == TEXT_PRINT_CLEAN && is_terminal_hostile_codepoint(cp)) {
            print_unicode_escape(cp);
        } else {
            write_utf8_bytes_to_stdout(data + pos, (size_t)consumed);
        }

        pos += consumed;
    }

    if (pos < byte_length) {
        fputs("...", stdout);
    }
    putchar('"');
}

static void print_binary_preview(const uint8_t* data, int64_t byte_length, size_t max_bytes) {
    int64_t limit;
    int64_t i;

    if (data == NULL && byte_length > 0) {
        printf("<null-data>");
        return;
    }

    if (byte_length < 0) {
        printf("<bad-length>");
        return;
    }

    limit = byte_length;
    if ((uint64_t)limit > (uint64_t)max_bytes) {
        limit = (int64_t)max_bytes;
    }

    fputs("0x", stdout);
    for (i = 0; i < limit; ++i) {
        printf("%02X", (unsigned)data[i]);
    }
    if (limit < byte_length) {
        fputs("...", stdout);
    }
}

static int read_integer_index(const struct ArrowSchema* schema,
                              const struct ArrowArray* array,
                              int64_t logical_index,
                              int64_t* out_value) {
    const char* format = safe_format(schema);
    int64_t i;

    if (array == NULL || array->buffers == NULL || array->buffers[1] == NULL || out_value == NULL) {
        return 0;
    }

    i = value_index(array, logical_index);

    if (strcmp(format, "c") == 0) {
        *out_value = (int64_t)((const int8_t*)array->buffers[1])[i];
        return 1;
    }
    if (strcmp(format, "C") == 0) {
        *out_value = (int64_t)((const uint8_t*)array->buffers[1])[i];
        return 1;
    }
    if (strcmp(format, "s") == 0) {
        *out_value = (int64_t)((const int16_t*)array->buffers[1])[i];
        return 1;
    }
    if (strcmp(format, "S") == 0) {
        *out_value = (int64_t)((const uint16_t*)array->buffers[1])[i];
        return 1;
    }
    if (strcmp(format, "i") == 0) {
        *out_value = (int64_t)((const int32_t*)array->buffers[1])[i];
        return 1;
    }
    if (strcmp(format, "I") == 0) {
        *out_value = (int64_t)((const uint32_t*)array->buffers[1])[i];
        return 1;
    }
    if (strcmp(format, "l") == 0) {
        *out_value = ((const int64_t*)array->buffers[1])[i];
        return 1;
    }
    if (strcmp(format, "L") == 0) {
        uint64_t value = ((const uint64_t*)array->buffers[1])[i];
        if (value > INT64_MAX) {
            return 0;
        }
        *out_value = (int64_t)value;
        return 1;
    }

    return 0;
}

static int schema_name_is(const struct ArrowSchema* schema, const char* expected) {
    return schema != NULL && schema->name != NULL && strcmp(schema->name, expected) == 0;
}

static int64_t find_bytes_from(const char* data,
                               int64_t byte_length,
                               int64_t start,
                               int64_t max_scan,
                               const char* needle,
                               int64_t needle_length) {
    int64_t last;
    int64_t i;

    if (data == NULL || needle == NULL || byte_length < 0 || start < 0 || needle_length <= 0) {
        return -1;
    }
    if (start >= byte_length || needle_length > byte_length) {
        return -1;
    }

    last = byte_length - needle_length;
    if (max_scan >= 0 && start + max_scan < last) {
        last = start + max_scan;
    }

    for (i = start; i <= last; ++i) {
        if (memcmp(data + i, needle, (size_t)needle_length) == 0) {
            return i;
        }
    }

    return -1;
}

static void maybe_skip_fineweb_text_metadata(const struct ArrowSchema* schema,
                                             const char** in_out_data,
                                             int64_t* in_out_length) {
    const char* data;
    int64_t length;
    int64_t pos;
    int64_t skip = 0;

    if (!schema_name_is(schema, "text") || in_out_data == NULL || in_out_length == NULL) {
        return;
    }

    data = *in_out_data;
    length = *in_out_length;
    if (data == NULL || length <= 0) {
        return;
    }

    /* FineWeb rows often start with a YAML-ish metadata block:
       ---\nmeta...\n---\n. Skip it so the preview starts closer to page text. */
    if (length >= 4 && data[0] == '-' && data[1] == '-' && data[2] == '-') {
        pos = find_bytes_from(data, length, 3, 8192, "\n---\n", 5);
        if (pos >= 0) {
            skip = pos + 5;
        } else {
            pos = find_bytes_from(data, length, 3, 8192, "\r\n---\r\n", 7);
            if (pos >= 0) {
                skip = pos + 7;
            }
        }
    }

    while (skip < length && (data[skip] == ' ' || data[skip] == '\t' ||
                             data[skip] == '\r' || data[skip] == '\n')) {
        ++skip;
    }

    /* Some crawled rows contain a dangling HTML-comment terminator after metadata. */
    if (skip + 3 <= length && data[skip] == '-' && data[skip + 1] == '-' && data[skip + 2] == '>') {
        skip += 3;
        while (skip < length && (data[skip] == ' ' || data[skip] == '\t' ||
                                 data[skip] == '\r' || data[skip] == '\n')) {
            ++skip;
        }
    }

    if (skip > 0 && skip < length) {
        *in_out_data = data + skip;
        *in_out_length = length - skip;
    }
}

static void print_arrow_value(const struct ArrowSchema* schema,
                              const struct ArrowArray* array,
                              int64_t logical_index,
                              size_t string_limit_bytes,
                              TextPrintMode text_mode) {
    const char* format = safe_format(schema);
    int64_t i;

    if (schema == NULL || array == NULL) {
        printf("<missing>");
        return;
    }

    if (!arrow_is_valid(array, logical_index)) {
        printf("null");
        return;
    }

    if (schema->dictionary != NULL && array->dictionary != NULL) {
        int64_t dictionary_index = 0;
        if (!read_integer_index(schema, array, logical_index, &dictionary_index)) {
            printf("<bad-dictionary-index>");
            return;
        }
        if (dictionary_index < 0 || dictionary_index >= array->dictionary->length) {
            printf("<dictionary-index-out-of-range:%" PRId64 ">", dictionary_index);
            return;
        }
        print_arrow_value(schema->dictionary,
                          array->dictionary,
                          dictionary_index,
                          string_limit_bytes,
                          text_mode);
        return;
    }

    i = value_index(array, logical_index);

    if (strcmp(format, "n") == 0) {
        printf("null");
    } else if (strcmp(format, "b") == 0) {
        const uint8_t* values = array->buffers != NULL ? (const uint8_t*)array->buffers[1] : NULL;
        printf("%s", bitmap_value(values, i) ? "true" : "false");
    } else if (strcmp(format, "c") == 0) {
        printf("%" PRId8, ((const int8_t*)array->buffers[1])[i]);
    } else if (strcmp(format, "C") == 0) {
        printf("%" PRIu8, ((const uint8_t*)array->buffers[1])[i]);
    } else if (strcmp(format, "s") == 0) {
        printf("%" PRId16, ((const int16_t*)array->buffers[1])[i]);
    } else if (strcmp(format, "S") == 0) {
        printf("%" PRIu16, ((const uint16_t*)array->buffers[1])[i]);
    } else if (strcmp(format, "i") == 0) {
        printf("%" PRId32, ((const int32_t*)array->buffers[1])[i]);
    } else if (strcmp(format, "I") == 0) {
        printf("%" PRIu32, ((const uint32_t*)array->buffers[1])[i]);
    } else if (strcmp(format, "l") == 0) {
        printf("%" PRId64, ((const int64_t*)array->buffers[1])[i]);
    } else if (strcmp(format, "L") == 0) {
        printf("%" PRIu64, ((const uint64_t*)array->buffers[1])[i]);
    } else if (strcmp(format, "f") == 0) {
        printf("%.9g", ((const float*)array->buffers[1])[i]);
    } else if (strcmp(format, "g") == 0) {
        printf("%.17g", ((const double*)array->buffers[1])[i]);
    } else if (strcmp(format, "u") == 0) {
        const int32_t* offsets;
        const char* data;
        int64_t start;
        int64_t end;

        if (array->buffers == NULL || array->buffers[1] == NULL || array->buffers[2] == NULL) {
            printf("<bad-utf8-array>");
            return;
        }

        offsets = (const int32_t*)array->buffers[1];
        data = (const char*)array->buffers[2];
        start = (int64_t)offsets[i];
        end = (int64_t)offsets[i + 1];
        {
            const char* value_data = data + start;
            int64_t value_length = end - start;
            maybe_skip_fineweb_text_metadata(schema, &value_data, &value_length);
            print_escaped_utf8(value_data, value_length, string_limit_bytes, text_mode);
        }
    } else if (strcmp(format, "U") == 0) {
        const int64_t* offsets;
        const char* data;
        int64_t start;
        int64_t end;

        if (array->buffers == NULL || array->buffers[1] == NULL || array->buffers[2] == NULL) {
            printf("<bad-large-utf8-array>");
            return;
        }

        offsets = (const int64_t*)array->buffers[1];
        data = (const char*)array->buffers[2];
        start = offsets[i];
        end = offsets[i + 1];
        {
            const char* value_data = data + start;
            int64_t value_length = end - start;
            maybe_skip_fineweb_text_metadata(schema, &value_data, &value_length);
            print_escaped_utf8(value_data, value_length, string_limit_bytes, text_mode);
        }
    } else if (strcmp(format, "z") == 0) {
        const int32_t* offsets;
        const uint8_t* data;
        int64_t start;
        int64_t end;

        if (array->buffers == NULL || array->buffers[1] == NULL || array->buffers[2] == NULL) {
            printf("<bad-binary-array>");
            return;
        }

        offsets = (const int32_t*)array->buffers[1];
        data = (const uint8_t*)array->buffers[2];
        start = (int64_t)offsets[i];
        end = (int64_t)offsets[i + 1];
        print_binary_preview(data + start, end - start, string_limit_bytes);
    } else if (strcmp(format, "Z") == 0) {
        const int64_t* offsets;
        const uint8_t* data;
        int64_t start;
        int64_t end;

        if (array->buffers == NULL || array->buffers[1] == NULL || array->buffers[2] == NULL) {
            printf("<bad-large-binary-array>");
            return;
        }

        offsets = (const int64_t*)array->buffers[1];
        data = (const uint8_t*)array->buffers[2];
        start = offsets[i];
        end = offsets[i + 1];
        print_binary_preview(data + start, end - start, string_limit_bytes);
    } else if (strncmp(format, "tdD", 3) == 0) {
        printf("date32_days(%" PRId32 ")", ((const int32_t*)array->buffers[1])[i]);
    } else if (strncmp(format, "tdm", 3) == 0) {
        printf("date64_ms(%" PRId64 ")", ((const int64_t*)array->buffers[1])[i]);
    } else if (strncmp(format, "ts", 2) == 0) {
        printf("timestamp_raw(%" PRId64 ")", ((const int64_t*)array->buffers[1])[i]);
    } else {
        printf("<unsupported format '%s'>", format[0] ? format : "(empty)");
    }
}

static void print_schema_columns(const struct ArrowSchema* schema) {
    int64_t column;

    if (schema == NULL || schema->children == NULL) {
        printf("schema columns: <unavailable>\n");
        return;
    }

    printf("schema columns (%" PRId64 "):\n", schema->n_children);
    for (column = 0; column < schema->n_children; ++column) {
        const struct ArrowSchema* field = schema->children[column];
        printf("  %" PRId64 ": %s  format=%s\n",
               column,
               safe_name(schema, column),
               safe_format(field));
    }
}

static void print_record_batch_rows(const struct ArrowSchema* schema,
                                    const struct ArrowArray* batch,
                                    int64_t first_row,
                                    int64_t row_count,
                                    int64_t global_row_offset,
                                    size_t string_limit_bytes,
                                    TextPrintMode text_mode) {
    int64_t row;
    int64_t columns;

    if (schema == NULL || batch == NULL || schema->children == NULL || batch->children == NULL) {
        printf("<cannot print batch: missing schema or children>\n");
        return;
    }

    columns = schema->n_children < batch->n_children ? schema->n_children : batch->n_children;

    for (row = first_row; row < first_row + row_count; ++row) {
        int64_t column;
        printf("\nrow %" PRId64 ":\n", global_row_offset + row);
        for (column = 0; column < columns; ++column) {
            const struct ArrowSchema* field_schema = schema->children[column];
            const struct ArrowArray* field_array = batch->children[column];

            printf("  %s: ", safe_name(schema, column));
            print_arrow_value(field_schema, field_array, row, string_limit_bytes, text_mode);
            putchar('\n');
        }
    }
}

int main(int argc, char** argv) {
    const char* path = argc > 1 ? argv[1] : "input.parquet";
    size_t max_sample_rows = argc > 2 ? parse_size_arg(argv[2], DEFAULT_SAMPLE_ROWS) : DEFAULT_SAMPLE_ROWS;
    size_t batch_size = argc > 3 ? parse_size_arg(argv[3], DEFAULT_BATCH_SIZE) : DEFAULT_BATCH_SIZE;
    size_t string_limit_bytes = argc > 4 ? parse_size_arg(argv[4], DEFAULT_STRING_LIMIT_BYTES) : DEFAULT_STRING_LIMIT_BYTES;
    TextPrintMode text_mode = argc > 5 ? parse_text_mode(argv[5]) : TEXT_PRINT_CLEAN;

    ParquetArrowFileInfo info = {0};
    struct ArrowArrayStream stream = {0};
    struct ArrowSchema schema = {0};
    int64_t global_row_offset = 0;
    size_t printed_rows = 0;
    size_t batch_count = 0;

    configure_process_for_utf8_output();

    printf("opening: %s\n", path);
    printf("sample rows: %zu, batch size: %zu, string preview bytes: %zu, text mode: %s\n",
           max_sample_rows,
           batch_size,
           string_limit_bytes,
           text_mode_name(text_mode));

    check_api(parquet_arrow_file_info(path, &info));
    printf("file info: rows=%" PRId64 ", row_groups=%zu, columns=%zu\n",
           info.num_rows,
           info.num_row_groups,
           info.num_columns);

    check_api(parquet_arrow_read_file(path, batch_size, &stream));
    check_stream(stream.get_schema(&stream, &schema), &stream);
    print_schema_columns(&schema);

    while (printed_rows < max_sample_rows) {
        struct ArrowArray batch = {0};
        int64_t rows_to_print;

        check_stream(stream.get_next(&stream, &batch), &stream);
        if (batch.release == NULL) {
            break;
        }

        ++batch_count;
        rows_to_print = batch.length;
        if ((uint64_t)rows_to_print > (uint64_t)(max_sample_rows - printed_rows)) {
            rows_to_print = (int64_t)(max_sample_rows - printed_rows);
        }

        print_record_batch_rows(&schema,
                                &batch,
                                0,
                                rows_to_print,
                                global_row_offset,
                                string_limit_bytes,
                                text_mode);

        printed_rows += (size_t)rows_to_print;
        global_row_offset += batch.length;

        batch.release(&batch);
    }

    if (schema.release != NULL) {
        schema.release(&schema);
    }
    parquet_arrow_release_stream(&stream);

    printf("\nfinished: batches_touched=%zu, rows_printed=%zu\n", batch_count, printed_rows);
    return 0;
}
