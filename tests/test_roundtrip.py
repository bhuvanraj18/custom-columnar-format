# tests/test_roundtrip.py

import sys, os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import csv
from writer import write_scbf
from reader import read_all

def write_sample_csv(path):
    rows = [
        ["id", "name", "score"],
        ["1", "Alice", "91.5"],
        ["2", "Bob", "88.0"],
        ["3", "Charlie", "79.25"],
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerows(rows)

def test_roundtrip():
    csv_in = "tests/sample.csv"
    bin_out = "tests/sample.scbf"
    csv_out = "tests/sample_out.csv"

    write_sample_csv(csv_in)
    print("[1] CSV created.")

    write_scbf(csv_in, bin_out)
    print("[2] SCBF written.")

    names, rows = read_all(bin_out)
    print("[3] SCBF read successfully.")

    with open(csv_out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(names)
        w.writerows(rows)
    print("[4] CSV restored.")

    with open(csv_in, "r", encoding="utf-8") as f1, open(csv_out, "r", encoding="utf-8") as f2:
        assert f1.read().strip() == f2.read().strip()

    print("✔ Round-trip test passed!")

# IMPORTANT: RUN THE TEST
test_roundtrip()
