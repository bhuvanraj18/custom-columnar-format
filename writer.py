import csv
import json
import struct
import zlib

MAGIC = b"SCBFv1\x00\x00"

TYPE_INT32 = 1
TYPE_FLOAT64 = 2
TYPE_UTF8 = 3

def infer_type(value):
    """Infer type based on first non-empty value."""
    if value == "":
        return str
    try:
        int(value)
        return int
    except:
        pass
    try:
        float(value)
        return float
    except:
        pass
    return str

def write_scbf(csv_path, out_path):
    # --- 1. Read CSV file ---
    with open(csv_path, "r", newline="", encoding="utf-8") as f:
        reader = list(csv.reader(f))
    
    header = reader[0]
    rows = reader[1:]

    num_rows = len(rows)
    num_cols = len(header)

    # --- 2. Infer schema from first row ---
    col_types = []
    for col_idx in range(num_cols):
        for row in rows:
            if row[col_idx] != "":
                inferred = infer_type(row[col_idx])
                break
        else:
            inferred = str

        if inferred is int:
            col_types.append(TYPE_INT32)
        elif inferred is float:
            col_types.append(TYPE_FLOAT64)
        else:
            col_types.append(TYPE_UTF8)

    # Build schema JSON
    schema = {
        "columns": [
            {"name": header[i],
             "type": "int32" if col_types[i] == TYPE_INT32 else
                     "float64" if col_types[i] == TYPE_FLOAT64 else
                     "utf8"}
            for i in range(num_cols)
        ]
    }
    schema_bytes = json.dumps(schema).encode("utf-8")

    # Placeholder for column metadata
    col_metadata = []

    # Space for writing actual data blocks
    data_blocks = []

    # Current file offset after header + metadata
    # Compute later
    offset_cursor = 0

    # --- 3. Build and compress column blocks ---
    for c in range(num_cols):
        col_type = col_types[c]
        name = header[c]

        if col_type == TYPE_INT32:
            buf = b"".join(struct.pack("<i", int(r[c])) for r in rows)
            uncomp = buf
            comp = zlib.compress(uncomp)
            metadata = {
                "name": name,
                "type": TYPE_INT32,
                "count": num_rows,
                "uncomp_size": len(uncomp),
                "comp_size": len(comp),
                "data": comp
            }
            col_metadata.append(metadata)

        elif col_type == TYPE_FLOAT64:
            buf = b"".join(struct.pack("<d", float(r[c])) for r in rows)
            uncomp = buf
            comp = zlib.compress(uncomp)
            metadata = {
                "name": name,
                "type": TYPE_FLOAT64,
                "count": num_rows,
                "uncomp_size": len(uncomp),
                "comp_size": len(comp),
                "data": comp
            }
            col_metadata.append(metadata)

        else:  # UTF-8 strings
            strings = [r[c] for r in rows]
            encoded = [s.encode("utf-8") for s in strings]
            all_bytes = b"".join(encoded)

            # Offsets
            offsets = [0]
            for s in encoded:
                offsets.append(offsets[-1] + len(s))

            offsets_buf = b"".join(struct.pack("<I", x) for x in offsets)
            off_comp = zlib.compress(offsets_buf)
            str_comp = zlib.compress(all_bytes)

            metadata = {
                "name": name,
                "type": TYPE_UTF8,
                "count": num_rows,
                "off_uncomp": len(offsets_buf),
                "off_comp": len(off_comp),
                "str_uncomp": len(all_bytes),
                "str_comp": len(str_comp),
                "off_data": off_comp,
                "str_data": str_comp
            }
            col_metadata.append(metadata)

    # --- 4. Calculate metadata table offset ---
    header_size = (
        len(MAGIC) +
        4 +                # schema_len
        len(schema_bytes) +
        4 +                # num_columns
        8 +                # total_rows
        8                  # meta_table_offset (written later)
    )

    # We don't know metadata size yet, but we know the header ends before metadata.

    # --- 5. Write file ---
    with open(out_path, "wb") as f:
        # Write header (meta_table_offset = 0 for now)
        f.write(MAGIC)
        f.write(struct.pack("<I", len(schema_bytes)))
        f.write(schema_bytes)
        f.write(struct.pack("<I", num_cols))
        f.write(struct.pack("<Q", num_rows))
        f.write(struct.pack("<Q", 0))  # placeholder

        meta_start = f.tell()

        # --- Write column metadata ---
        col_offsets = []  # store block offsets to fill in later

        for meta in col_metadata:
            name = meta["name"].encode("utf-8")
            f.write(struct.pack("<H", len(name)))
            f.write(name)
            f.write(struct.pack("<B", meta["type"]))
            f.write(struct.pack("<Q", meta["count"]))

            if meta["type"] in (TYPE_INT32, TYPE_FLOAT64):
                f.write(struct.pack("<Q", meta["uncomp_size"]))
                f.write(struct.pack("<Q", meta["comp_size"]))
                col_offsets.append(f.tell())
                f.write(struct.pack("<Q", 0))  # placeholder

            else:  # UTF-8
                f.write(struct.pack("<Q", meta["off_uncomp"]))
                f.write(struct.pack("<Q", meta["off_comp"]))
                col_offsets.append(f.tell())
                f.write(struct.pack("<Q", 0))  # offset for offsets block
                f.write(struct.pack("<Q", meta["str_uncomp"]))
                f.write(struct.pack("<Q", meta["str_comp"]))
                f.write(struct.pack("<Q", 0))  # offset for string block

        data_start = f.tell()

        # --- Write column blocks and fill offsets ---
        offset_ptr = 0
        for i, meta in enumerate(col_metadata):
            pos = f.tell()

            # Fill offset
            f.seek(col_offsets[i])
            f.write(struct.pack("<Q", pos))

            f.seek(pos)

            if meta["type"] in (TYPE_INT32, TYPE_FLOAT64):
                f.write(meta["data"])

            else:
                # offsets block
                pos2 = f.tell()
                f.write(meta["off_data"])
                # write its offset in STR section
                f.seek(col_offsets[i] + 8)
                f.write(struct.pack("<Q", pos2))
                f.seek(pos2 + len(meta["off_data"]))

                # string data block
                pos3 = f.tell()
                f.write(meta["str_data"])
                f.seek(col_offsets[i] + 8 + 8 + 8)  # correct str_offset loc
                f.write(struct.pack("<Q", pos3))
                f.seek(pos3 + len(meta["str_data"]))

        end_pos = f.tell()

              # Go back and fix metadata table offset
        f.seek(len(MAGIC) + 4 + len(schema_bytes) + 4 + 8)
        f.write(struct.pack("<Q", meta_start))

    print("SCBF file written:", out_path)

