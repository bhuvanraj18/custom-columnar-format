# reader.py
import struct
import json
import zlib
from typing import List, Dict, Any, Tuple

MAGIC = b"SCBFv1\x00\x00"
TYPE_INT32 = 1
TYPE_FLOAT64 = 2
TYPE_UTF8 = 3

def read_u16(b): return struct.unpack("<H", b)[0]
def read_u32(b): return struct.unpack("<I", b)[0]
def read_u64(b): return struct.unpack("<Q", b)[0]
def read_u8(b):  return struct.unpack("<B", b)[0]

def read_header(f) -> Tuple[dict, int, int, int]:
    """
    Return: (schema_dict, num_columns, total_rows, meta_table_offset)
    Assumes file pointer at 0.
    """
    f.seek(0)
    magic = f.read(8)
    if magic != MAGIC:
        raise ValueError("Bad magic header: not an SCBF file")

    schema_len = read_u32(f.read(4))
    schema_json = f.read(schema_len).decode("utf-8")
    schema = json.loads(schema_json)

    num_columns = read_u32(f.read(4))
    total_rows = read_u64(f.read(8))
    meta_table_offset = read_u64(f.read(8))

    return schema, num_columns, total_rows, meta_table_offset

def read_meta_table(f, meta_table_offset: int, num_columns: int) -> List[dict]:
    """
    Parse the metadata table. Returns list of dicts describing each column.
    Each dict has keys: name, type (int32/float64/utf8), count, and the sizes/offsets.
    """
    metas = []
    f.seek(meta_table_offset)
    for _ in range(num_columns):
        name_len = read_u16(f.read(2))
        name = f.read(name_len).decode("utf-8")
        type_code = read_u8(f.read(1))
        count = read_u64(f.read(8))

        if type_code in (TYPE_INT32, TYPE_FLOAT64):
            uncomp_size = read_u64(f.read(8))
            comp_size = read_u64(f.read(8))
            block_offset = read_u64(f.read(8))
            metas.append({
                "name": name, "type": ("int32" if type_code == TYPE_INT32 else "float64"),
                "count": count,
                "uncomp_size": uncomp_size, "comp_size": comp_size, "block_offset": block_offset
            })
        elif type_code == TYPE_UTF8:
            off_uncomp = read_u64(f.read(8))
            off_comp = read_u64(f.read(8))
            off_offset = read_u64(f.read(8))
            str_uncomp = read_u64(f.read(8))
            str_comp = read_u64(f.read(8))
            str_offset = read_u64(f.read(8))
            metas.append({
                "name": name, "type": "utf8", "count": count,
                "off_uncomp": off_uncomp, "off_comp": off_comp, "off_offset": off_offset,
                "str_uncomp": str_uncomp, "str_comp": str_comp, "str_offset": str_offset
            })
        else:
            raise ValueError(f"Unknown type code {type_code} for column {name}")
    return metas

def read_int32_column(f, meta: dict) -> List[int]:
    f.seek(meta["block_offset"])
    comp = f.read(meta["comp_size"])
    uncomp = zlib.decompress(comp)
    count = meta["count"]
    vals = [struct.unpack_from("<i", uncomp, i*4)[0] for i in range(count)]
    return vals

def read_float64_column(f, meta: dict) -> List[float]:
    f.seek(meta["block_offset"])
    comp = f.read(meta["comp_size"])
    uncomp = zlib.decompress(comp)
    count = meta["count"]
    vals = [struct.unpack_from("<d", uncomp, i*8)[0] for i in range(count)]
    return vals

def read_utf8_column(f, meta: dict) -> List[str]:
    # read offsets block
    f.seek(meta["off_offset"])
    off_comp = f.read(meta["off_comp"])
    off_uncomp = zlib.decompress(off_comp)
    # read string block
    f.seek(meta["str_offset"])
    str_comp = f.read(meta["str_comp"])
    str_uncomp = zlib.decompress(str_comp)

    count = meta["count"]
    # offsets are u32, (count+1) entries
    offsets = [struct.unpack_from("<I", off_uncomp, i*4)[0] for i in range(count+1)]
    res = []
    for i in range(count):
        a = offsets[i]
        b = offsets[i+1]
        res.append(str_uncomp[a:b].decode("utf-8"))
    return res

def read_columns(file_path: str, columns: List[str]) -> Dict[str, List[Any]]:
    """
    Read only the named columns and return a dict {colname: values_list}
    """
    with open(file_path, "rb") as f:
        schema, num_cols, total_rows, meta_table_offset = read_header(f)
        metas = read_meta_table(f, meta_table_offset, num_cols)

        # Build name->meta map
        meta_map = {m["name"]: m for m in metas}

        result = {}
        for col in columns:
            if col not in meta_map:
                raise KeyError(f"Column {col} not found")
            meta = meta_map[col]
            if meta["type"] == "int32":
                result[col] = read_int32_column(f, meta)
            elif meta["type"] == "float64":
                result[col] = read_float64_column(f, meta)
            elif meta["type"] == "utf8":
                result[col] = read_utf8_column(f, meta)
        return result

def read_all(file_path: str) -> Tuple[List[str], List[List[Any]]]:
    """
    Read whole file and return (column_names, rows_as_list_of_lists)
    """
    with open(file_path, "rb") as f:
        schema, num_cols, total_rows, meta_table_offset = read_header(f)
        metas = read_meta_table(f, meta_table_offset, num_cols)

        # read columns one by one
        cols_data = []
        names = []
        for meta in metas:
            names.append(meta["name"])
            if meta["type"] == "int32":
                cols_data.append(read_int32_column(f, meta))
            elif meta["type"] == "float64":
                cols_data.append(read_float64_column(f, meta))
            elif meta["type"] == "utf8":
                cols_data.append(read_utf8_column(f, meta))

        # reconstruct rows
        rows = []
        for i in range(total_rows):
            row = [cols_data[c][i] for c in range(len(cols_data))]
            rows.append(row)

        return names, rows

# Simple CLI demo if run directly
if __name__ == "__main__":
    import argparse, csv, sys
    parser = argparse.ArgumentParser(description="SCBF reader demo")
    parser.add_argument("file", help="SCBF file path")
    parser.add_argument("--cols", nargs="*", help="Columns to read (default: all)")
    parser.add_argument("--out", help="Optional CSV output path")
    args = parser.parse_args()

    if args.cols:
        data = read_columns(args.file, args.cols)
        # print simple preview
        for k, v in data.items():
            print(f"--- Column: {k} ({len(v)} rows) ---")
            print(v[:10])
        if args.out:
            # write columns to CSV with header cols
            cols = args.cols
            rows = list(zip(*(data[c] for c in cols)))
            with open(args.out, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(cols)
                w.writerows(rows)
            print("Wrote CSV:", args.out)
    else:
        names, rows = read_all(args.file)
        print("Columns:", names)
        print("First 10 rows:")
        for r in rows[:10]:
            print(r)
        if args.out:
            with open(args.out, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(names)
                w.writerows(rows)
            print("Wrote CSV:", args.out)
