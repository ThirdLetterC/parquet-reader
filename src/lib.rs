//! C ABI wrapper for reading and writing Parquet files as Arrow C Stream data.
//!
//! Build this crate as a cdylib/staticlib and include `parquet_arrow_ffi.h` from C.
//! The C-facing API intentionally uses the Arrow C Data / C Stream Interface instead
//! of exposing Rust-owned Arrow internals.

use std::cell::RefCell;
use std::ffi::{CStr, CString};
use std::fs::File;
use std::os::raw::{c_char, c_int};
use std::panic::{self, AssertUnwindSafe};
use std::path::PathBuf;
use std::ptr;

use arrow::array::RecordBatchReader;
use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::{BrotliLevel, Compression, GzipLevel, ZstdLevel};
use parquet::file::properties::WriterProperties;

/// Function completed successfully.
pub const PARQUET_ARROW_OK: c_int = 0;
/// Function failed; call parquet_arrow_last_error_message() for details.
pub const PARQUET_ARROW_ERROR: c_int = -1;
/// A Rust panic was caught before crossing the C ABI boundary.
pub const PARQUET_ARROW_PANIC: c_int = -2;

/// Compression constants accepted by parquet_arrow_write_file().
pub const PARQUET_ARROW_COMPRESSION_UNCOMPRESSED: c_int = 0;
pub const PARQUET_ARROW_COMPRESSION_SNAPPY: c_int = 1;
pub const PARQUET_ARROW_COMPRESSION_GZIP: c_int = 2;
pub const PARQUET_ARROW_COMPRESSION_BROTLI: c_int = 3;
pub const PARQUET_ARROW_COMPRESSION_ZSTD: c_int = 4;
pub const PARQUET_ARROW_COMPRESSION_LZ4_RAW: c_int = 5;

/// Basic Parquet file information returned by parquet_arrow_file_info().
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct ParquetArrowFileInfo {
    pub num_rows: i64,
    pub num_row_groups: usize,
    pub num_columns: usize,
}

type FfiResult<T = ()> = Result<T, String>;

thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = const { RefCell::new(None) };
}

fn set_last_error(message: impl Into<String>) {
    let sanitized = message.into().replace('\0', "\\0");
    let c_message = CString::new(sanitized)
        .unwrap_or_else(|_| CString::new("error message contained an interior NUL byte").unwrap());
    LAST_ERROR.with(|slot| {
        *slot.borrow_mut() = Some(c_message);
    });
}

fn clear_last_error_impl() {
    LAST_ERROR.with(|slot| {
        *slot.borrow_mut() = None;
    });
}

fn ffi_guard<F>(f: F) -> c_int
where
    F: FnOnce() -> FfiResult<()>,
{
    match panic::catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(())) => {
            clear_last_error_impl();
            PARQUET_ARROW_OK
        }
        Ok(Err(message)) => {
            set_last_error(message);
            PARQUET_ARROW_ERROR
        }
        Err(payload) => {
            let message = if let Some(s) = payload.downcast_ref::<&str>() {
                format!("panic inside parquet_arrow_ffi: {s}")
            } else if let Some(s) = payload.downcast_ref::<String>() {
                format!("panic inside parquet_arrow_ffi: {s}")
            } else {
                "panic inside parquet_arrow_ffi".to_string()
            };
            set_last_error(message);
            PARQUET_ARROW_PANIC
        }
    }
}

unsafe fn path_from_c(path: *const c_char) -> FfiResult<PathBuf> {
    if path.is_null() {
        return Err("path pointer is NULL".to_string());
    }

    let path_str = unsafe { CStr::from_ptr(path) }
        .to_str()
        .map_err(|e| format!("path must be valid UTF-8: {e}"))?;

    Ok(PathBuf::from(path_str))
}

fn compression_from_code(code: c_int) -> FfiResult<Compression> {
    match code {
        PARQUET_ARROW_COMPRESSION_UNCOMPRESSED => Ok(Compression::UNCOMPRESSED),
        PARQUET_ARROW_COMPRESSION_SNAPPY => Ok(Compression::SNAPPY),
        PARQUET_ARROW_COMPRESSION_GZIP => Ok(Compression::GZIP(GzipLevel::default())),
        PARQUET_ARROW_COMPRESSION_BROTLI => Ok(Compression::BROTLI(BrotliLevel::default())),
        PARQUET_ARROW_COMPRESSION_ZSTD => Ok(Compression::ZSTD(ZstdLevel::default())),
        PARQUET_ARROW_COMPRESSION_LZ4_RAW => Ok(Compression::LZ4_RAW),
        _ => Err(format!(
            "unsupported compression code {code}; use one of 0=UNCOMPRESSED, 1=SNAPPY, 2=GZIP, 3=BROTLI, 4=ZSTD, 5=LZ4_RAW"
        )),
    }
}

/// Return the last error message for the current thread.
///
/// The returned pointer is owned by Rust and remains valid until the next FFI call on
/// the same thread that changes/clears the last error. Do not free it.
#[no_mangle]
pub extern "C" fn parquet_arrow_last_error_message() -> *const c_char {
    LAST_ERROR.with(|slot| match slot.borrow().as_ref() {
        Some(message) => message.as_ptr(),
        None => ptr::null(),
    })
}

/// Clear the last error message for the current thread.
#[no_mangle]
pub extern "C" fn parquet_arrow_clear_last_error() {
    clear_last_error_impl();
}

/// Free a string returned by this library, such as from parquet_arrow_schema_string().
///
/// Do not use this on parquet_arrow_last_error_message() because that pointer is not
/// heap-owned by the caller.
#[no_mangle]
pub unsafe extern "C" fn parquet_arrow_free_c_string(s: *mut c_char) {
    if !s.is_null() {
        unsafe {
            drop(CString::from_raw(s));
        }
    }
}

/// Convenience release function for streams produced by parquet_arrow_read_file().
///
/// Equivalent to calling stream->release(stream) when stream and stream->release are
/// non-NULL. It is safe to call on an already-released stream.
#[no_mangle]
pub unsafe extern "C" fn parquet_arrow_release_stream(stream: *mut FFI_ArrowArrayStream) {
    if stream.is_null() {
        return;
    }

    let release = unsafe { (*stream).release };
    if let Some(release_fn) = release {
        unsafe {
            release_fn(stream);
        }
    }
}

/// Fill out_info with basic Parquet metadata.
#[no_mangle]
pub unsafe extern "C" fn parquet_arrow_file_info(
    path: *const c_char,
    out_info: *mut ParquetArrowFileInfo,
) -> c_int {
    ffi_guard(|| {
        if out_info.is_null() {
            return Err("out_info pointer is NULL".to_string());
        }

        let path = unsafe { path_from_c(path) }?;
        let file =
            File::open(&path).map_err(|e| format!("failed to open '{}': {e}", path.display()))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| {
            format!(
                "failed to read Parquet metadata from '{}': {e}",
                path.display()
            )
        })?;
        let metadata = builder.metadata().file_metadata();
        let schema = builder.schema();

        unsafe {
            *out_info = ParquetArrowFileInfo {
                num_rows: metadata.num_rows(),
                num_row_groups: builder.metadata().num_row_groups(),
                num_columns: schema.fields().len(),
            };
        }

        Ok(())
    })
}

/// Return a debug-formatted Arrow schema for a Parquet file.
///
/// On success, *out_schema_text receives a newly allocated C string. Free it with
/// parquet_arrow_free_c_string().
#[no_mangle]
pub unsafe extern "C" fn parquet_arrow_schema_string(
    path: *const c_char,
    out_schema_text: *mut *mut c_char,
) -> c_int {
    ffi_guard(|| {
        if out_schema_text.is_null() {
            return Err("out_schema_text pointer is NULL".to_string());
        }
        unsafe {
            *out_schema_text = ptr::null_mut();
        }

        let path = unsafe { path_from_c(path) }?;
        let file =
            File::open(&path).map_err(|e| format!("failed to open '{}': {e}", path.display()))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| {
            format!(
                "failed to read Parquet schema from '{}': {e}",
                path.display()
            )
        })?;

        let schema_text = format!("{:#?}", builder.schema());
        let c_schema = CString::new(schema_text)
            .map_err(|e| format!("failed to allocate schema string: {e}"))?;

        unsafe {
            *out_schema_text = c_schema.into_raw();
        }

        Ok(())
    })
}

/// Open a Parquet file and export it as an ArrowArrayStream.
///
/// Arguments:
/// - path: UTF-8, NUL-terminated file path.
/// - batch_size: max rows per produced RecordBatch. Pass 0 for crate default.
/// - out_stream: caller-allocated ArrowArrayStream storage. On success, Rust writes a
///   valid stream into it. The caller must eventually call stream->release(stream) or
///   parquet_arrow_release_stream(out_stream).
#[no_mangle]
pub unsafe extern "C" fn parquet_arrow_read_file(
    path: *const c_char,
    batch_size: usize,
    out_stream: *mut FFI_ArrowArrayStream,
) -> c_int {
    ffi_guard(|| {
        if out_stream.is_null() {
            return Err("out_stream pointer is NULL".to_string());
        }

        let path = unsafe { path_from_c(path) }?;
        let file =
            File::open(&path).map_err(|e| format!("failed to open '{}': {e}", path.display()))?;
        let mut builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| {
            format!(
                "failed to create Parquet reader for '{}': {e}",
                path.display()
            )
        })?;

        if batch_size > 0 {
            builder = builder.with_batch_size(batch_size);
        }

        let reader = builder.build().map_err(|e| {
            format!(
                "failed to build Parquet batch reader for '{}': {e}",
                path.display()
            )
        })?;

        let stream = FFI_ArrowArrayStream::new(Box::new(reader));
        unsafe {
            ptr::write(out_stream, stream);
        }

        Ok(())
    })
}

/// Import an ArrowArrayStream from C and write it to a Parquet file.
///
/// Arguments:
/// - path: UTF-8, NUL-terminated output file path.
/// - input_stream: an ArrowArrayStream produced by C/C++/nanoarrow/etc.
/// - compression: one of PARQUET_ARROW_COMPRESSION_* constants.
/// - max_row_group_row_count: pass 0 for crate default; otherwise must be > 0.
///
/// Ownership note: ArrowArrayStreamReader::from_raw moves/consumes the stream content
/// from input_stream. After a successful call, C must not use or release input_stream.
#[no_mangle]
pub unsafe extern "C" fn parquet_arrow_write_file(
    path: *const c_char,
    input_stream: *mut FFI_ArrowArrayStream,
    compression: c_int,
    max_row_group_row_count: usize,
) -> c_int {
    ffi_guard(|| {
        if input_stream.is_null() {
            return Err("input_stream pointer is NULL".to_string());
        }

        let path = unsafe { path_from_c(path) }?;
        let mut reader = unsafe { ArrowArrayStreamReader::from_raw(input_stream) }
            .map_err(|e| format!("failed to import ArrowArrayStream from C: {e}"))?;

        let mut props_builder =
            WriterProperties::builder().set_compression(compression_from_code(compression)?);
        if max_row_group_row_count > 0 {
            props_builder =
                props_builder.set_max_row_group_row_count(Some(max_row_group_row_count));
        }
        let props = props_builder.build();

        let schema = reader.schema();
        let file = File::create(&path)
            .map_err(|e| format!("failed to create '{}': {e}", path.display()))?;
        let mut writer = ArrowWriter::try_new(file, schema, Some(props)).map_err(|e| {
            format!(
                "failed to create Parquet writer for '{}': {e}",
                path.display()
            )
        })?;

        for batch_result in reader {
            let batch = batch_result
                .map_err(|e| format!("failed to read input Arrow batch from C stream: {e}"))?;
            writer
                .write(&batch)
                .map_err(|e| format!("failed to write RecordBatch to '{}': {e}", path.display()))?;
        }

        writer
            .close()
            .map_err(|e| format!("failed to finish Parquet file '{}': {e}", path.display()))?;

        Ok(())
    })
}
