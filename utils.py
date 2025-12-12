# utils.py
import struct

# ---- Binary readers ----
def read_u8(f):
    return struct.unpack("<B", f.read(1))[0]

def read_u16(f):
    return struct.unpack("<H", f.read(2))[0]

def read_u32(f):
    return struct.unpack("<I", f.read(4))[0]

def read_u64(f):
    return struct.unpack("<Q", f.read(8))[0]

# ---- Binary writers ----
def write_u8(f, v):
    f.write(struct.pack("<B", v))

def write_u16(f, v):
    f.write(struct.pack("<H", v))

def write_u32(f, v):
    f.write(struct.pack("<I", v))

def write_u64(f, v):
    f.write(struct.pack("<Q", v))
