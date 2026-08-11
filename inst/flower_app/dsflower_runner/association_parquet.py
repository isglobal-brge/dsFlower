"""Bounded Parquet projection for the private association staging path."""

import hashlib
import os
import stat
import tempfile


_CONTRACT = "dsflower-association-parquet-projection/v1"


def runtime_ready():
    """Return whether the pinned runtime exposes the required Parquet API."""
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
        return bool(
            hasattr(pa, "types") and hasattr(pq, "ParquetFile") and
            pa.Codec.is_available("zstd"))
    except Exception:
        return False


def _positive_integer(value, name):
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError("%s must be one positive integer" % name)
    return value


def _regular_source_descriptor(path):
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags)
    info = os.fstat(descriptor)
    if not stat.S_ISREG(info.st_mode):
        os.close(descriptor)
        raise ValueError("association Parquet source must be a regular file")
    return descriptor


def _sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        while True:
            block = handle.read(1024 * 1024)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


def _logical_table_bytes(table):
    """Bound the R materialization size without expanding dictionaries."""
    import numpy as np
    import pyarrow as pa

    total = 0
    for column in table.columns:
        for chunk in column.chunks:
            if not pa.types.is_dictionary(chunk.type):
                total += int(chunk.nbytes)
                continue
            dictionary = chunk.dictionary
            if not (pa.types.is_string(dictionary.type) or
                    pa.types.is_large_string(dictionary.type) or
                    pa.types.is_binary(dictionary.type) or
                    pa.types.is_large_binary(dictionary.type)):
                total += int(chunk.nbytes)
                continue
            if len(dictionary) == 0:
                total += (len(chunk) + 1) * 8 + (len(chunk) + 7) // 8
                continue
            values = dictionary.to_pylist()
            lengths = np.fromiter((
                len(value.encode("utf-8")) if isinstance(value, str)
                else len(value) for value in values
            ), dtype=np.int64, count=len(dictionary))
            indices = chunk.indices.fill_null(0).to_numpy(
                zero_copy_only=False).astype(np.int64, copy=False)
            counts = np.bincount(indices, minlength=len(dictionary))
            if chunk.null_count and len(counts):
                counts[0] -= chunk.null_count
            total += int(np.dot(counts, lengths))
            total += (len(chunk) + 1) * 8 + (len(chunk) + 7) // 8
    return total


def materialize_bounded_projection(
        source, destination, columns, *, max_rows, max_bytes):
    """Project one Parquet handle after a pre-decode physical-size check."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    max_rows = _positive_integer(max_rows, "max_rows")
    max_bytes = _positive_integer(max_bytes, "max_bytes")
    if not isinstance(source, str) or not source or "\x00" in source or \
            not os.path.isabs(source):
        raise ValueError("association Parquet source path is invalid")
    if not isinstance(destination, str) or not destination or \
            "\x00" in destination or not os.path.isabs(destination):
        raise ValueError("association Parquet destination path is invalid")
    source = os.path.abspath(source)
    destination = os.path.abspath(destination)
    if source == destination or os.path.lexists(destination):
        raise ValueError("association Parquet destination must be new")
    parent = os.path.dirname(destination)
    if not os.path.isdir(parent) or os.path.islink(parent):
        raise ValueError("association Parquet destination parent is invalid")
    if not isinstance(columns, list) or not 1 <= len(columns) <= 3 or \
            any(not isinstance(value, str) or not value or "\x00" in value
                for value in columns) or len(set(columns)) != len(columns):
        raise ValueError("association Parquet columns are invalid")

    descriptor = _regular_source_descriptor(source)
    try:
        with os.fdopen(descriptor, "rb", closefd=True) as source_handle:
            descriptor = None
            parquet = pq.ParquetFile(source_handle, read_dictionary=columns)
            metadata = parquet.metadata
            rows = int(metadata.num_rows)
            if rows < 0 or rows > max_rows or rows * 64 > max_bytes:
                raise ValueError(
                    "association Parquet projection exceeds its physical cap")

            schema = parquet.schema_arrow
            names = list(schema.names)
            if len(names) != len(set(names)) or any(
                    name not in names for name in columns):
                raise ValueError("association Parquet columns are unavailable")
            indices = [names.index(name) for name in columns]
            if any(pa.types.is_nested(schema.field(index).type)
                   for index in indices):
                raise ValueError("association Parquet columns must be scalar")

            uncompressed = 0
            for row_group_index in range(metadata.num_row_groups):
                row_group = metadata.row_group(row_group_index)
                selected_chunks = {}
                for column_index in range(row_group.num_columns):
                    chunk = row_group.column(column_index)
                    path = chunk.path_in_schema
                    if path in columns:
                        if path in selected_chunks:
                            raise ValueError(
                                "association Parquet metadata is ambiguous")
                        selected_chunks[path] = chunk
                if set(selected_chunks) != set(columns):
                    raise ValueError(
                        "association Parquet columns are unavailable")
                for name in columns:
                    size = selected_chunks[name].total_uncompressed_size
                    if isinstance(size, bool) or not isinstance(size, int) or \
                            size < 0:
                        raise ValueError(
                            "association Parquet metadata is invalid")
                    uncompressed += size
                    if uncompressed + rows * 64 > max_bytes:
                        raise ValueError(
                            "association Parquet projection exceeds its physical cap")

            table = parquet.read(columns=columns, use_threads=False)
            table_bytes = int(table.nbytes)
            logical_bytes = _logical_table_bytes(table)
            if table.num_rows != rows or table.column_names != columns or \
                    table_bytes < 0 or logical_bytes < 0 or \
                    max(table_bytes, logical_bytes) + rows * 64 > max_bytes:
                raise ValueError(
                    "association Parquet projection exceeds its physical cap")
    finally:
        if descriptor is not None:
            os.close(descriptor)

    temporary_descriptor, temporary = tempfile.mkstemp(
        prefix=".association-projection-", suffix=".parquet", dir=parent)
    os.close(temporary_descriptor)
    try:
        pq.write_table(
            table, temporary, compression="zstd", use_dictionary=True)
        os.chmod(temporary, 0o600)
        file_bytes = os.path.getsize(temporary)
        if file_bytes < 1 or file_bytes > max_bytes:
            raise ValueError(
                "association Parquet projection exceeds its physical cap")
        sha256 = _sha256(temporary)
        if os.path.lexists(destination):
            raise ValueError("association Parquet destination must remain new")
        os.replace(temporary, destination)
        temporary = None
    finally:
        if temporary is not None:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass

    return {
        "contract": _CONTRACT,
        "file_bytes": file_bytes,
        "materialized_bytes": max(uncompressed, table_bytes, logical_bytes),
        "rows": rows,
        "sha256": sha256,
    }


__all__ = ["materialize_bounded_projection", "runtime_ready"]
