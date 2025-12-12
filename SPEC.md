```
# SPEC: Simple Columnar Binary Format (SCBF)

## 1. Overview

SCBF (Simple Columnar Binary Format) is a lightweight columnar storage format designed for efficient analytical processing.

Features:
- Column-wise storage
- Per-column zlib compression
- Selective column reading using byte offsets
- Supports `int32`, `float64`, and variable-length UTF-8 strings

All multi-byte values use little-endian encoding.

---

## 2. File Layout

The file consists of these parts, in order:

1. Header
2. Column metadata table
3. Column data blocks

---

## 3. Header Structure

Field | Type | Description
----- | ----- | -----------
magic | 8 bytes | ASCII "SCBFv1\0\0"
schema_len | u32 | Length of schema JSON
schema_json | variable | JSON describing columns
num_columns | u32 | Number of columns
total_rows | u64 | Total rows
meta_table_offset | u64 | Offset where column metadata begins

---

## 4. Schema JSON Example

{
  "columns": [
    {"name": "id", "type": "int32"},
    {"name": "name", "type": "utf8"},
    {"name": "score", "type": "float64"}
  ]
}

---

## 5. Column Metadata Table

Common metadata fields:

Field | Type | Description
----- | ----- | -----------
col_name_len | u16 | Length of UTF-8 column name
col_name | bytes | UTF-8 name
type_code | u8 | 1=int32, 2=float64, 3=utf8
count | u64 | Number of rows

### int32 Metadata
uncomp_size (u64)
comp_size (u64)
block_offset (u64)

### float64 Metadata
Same as int32.

### UTF-8 Metadata
off_uncomp_size (u64)
off_comp_size (u64)
off_offset (u64)
str_uncomp_size (u64)
str_comp_size (u64)
str_offset (u64)

Offsets contain (count + 1) u32 integers.

---

## 6. Column Data Blocks

Numeric:
- One compressed block

UTF-8:
- Compressed offsets block
- Compressed strings block

---

## 7. Compression

Uses zlib.compress and zlib.decompress.

---

## 8. Selective Column Reads

Reader performs:
1. Reads header
2. Reads metadata
3. Locates target column offsets
4. Reads only that column’s blocks
5. Decompresses values

---

## 9. Round-trip Guarantee

CSV -> SCBF -> CSV must produce identical data.

---

## 10. Optional Extensions

- Null bitmaps
- Dictionary encoding
- Statistics
- Checksum footer
```
