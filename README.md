# Simple Columnar Binary Format (SCBF)

A lightweight educational columnar binary format implemented in Python.  
SCBF demonstrates concepts used in Parquet/ORC: column-wise storage, compression, metadata indexing, and selective column reads.

Author: **Bhuvan Raj Patnaik (Roll: 23A91A1231)**

---

## Project Contents

```
custom_columnar_format/
│
├── SPEC.md                - Binary format specification
├── writer.py              - CSV → SCBF writer
├── reader.py              - SCBF reader and selective column APIs
├── cli.py                 - Command-line tools (csv_to_custom, custom_to_csv)
├── utils.py               - Binary helper functions
├── README.md              - This documentation
│
├── tests/
│   ├── test_roundtrip.py  - CSV → SCBF → CSV correctness test
│   └── bench_selective.py - Optional benchmark for selective column reads
│
└── examples/
    └── sample.csv         - Sample input file
```

---

## What SCBF Supports

✓ Column-wise binary storage  
✓ Per-column zlib compression  
✓ Selective column reads using stored offsets  
✓ Supported data types:
- int32  
- float64  
- utf8 strings (variable length)

✓ Round-trip guarantee (CSV → SCBF → CSV)  
✓ Little-endian encoding for all multi-byte values  

---

## Usage

### **Convert CSV → SCBF**
```bash
python cli.py csv_to_custom examples/sample.csv examples/sample.scbf
```

### **Convert SCBF → CSV (all columns)**
```bash
python cli.py custom_to_csv examples/sample.scbf restored.csv
```

### **Convert SCBF → CSV (only selected columns)**  
(If your CLI supports --cols)
```bash
python cli.py custom_to_csv examples/sample.scbf out.csv --cols id name
```

---

## Python API

### **Read Entire File**
```python
from reader import SCBFReader

r = SCBFReader("examples/sample.scbf")
names = r.list_columns()
rows = r.read_all()

print(names)
print(rows[:5])
```

### **Selective Column Read**
```python
from reader import SCBFReader

r = SCBFReader("examples/sample.scbf")
subset = r.read_columns(["id", "score"])

print(subset["id"][:10])
```

---

## Tests

### **Round-trip test**
```bash
python tests/test_roundtrip.py
```

Expected output:
```
[1] CSV created.
[2] SCBF written.
[3] SCBF read successfully.
[4] CSV restored.
✔ Round-trip test passed!
```

### **Optional selective read benchmark**
```bash
python tests/bench_selective.py
```

---

## Implementation Summary

### writer.py
- Reads CSV  
- Infers column types  
- Packs binary values  
  - int32 → struct.pack("<i")  
  - float64 → struct.pack("<d")  
  - utf8 → length-prefixed encoding  
- Compresses each column block using zlib  
- Writes header, metadata, and compressed column blocks  

### reader.py
- Parses header and metadata  
- Uses offsets to jump directly to required columns  
- Decompresses only selected columns  
- Reconstructs data into Python lists/dicts  

---

## SCBF Format Summary  
(Full details in **SPEC.md**)

**File Layout:**
```
Header
Metadata (JSON)
Compressed column blocks
```

**UTF-8 columns store:**
- length + bytes per row  
  OR  
- offsets + string blob (per SPEC.md design)

---

## Future Improvements

- NULL value support  
- Dictionary encoding for strings  
- Min/max per column  
- Footer with checksums  
- Parallel decompression  

---
