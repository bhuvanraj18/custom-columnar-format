# cli.py
import argparse
from writer import write_scbf
from reader import read_all, read_columns
import csv

def csv_to_custom_cmd(args):
    write_scbf(args.csv, args.out)
    print(f"[OK] Converted CSV → SCBF: {args.out}")

def custom_to_csv_cmd(args):
    if args.cols:
        data = read_columns(args.file, args.cols)
        rows = list(zip(*(data[c] for c in args.cols)))
        with open(args.out, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(args.cols)
            w.writerows(rows)
    else:
        names, rows = read_all(args.file)
        with open(args.out, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(names)
            w.writerows(rows)

    print(f"[OK] Converted SCBF → CSV: {args.out}")

def main():
    parser = argparse.ArgumentParser(description="SCBF Columnar Format Tools")
    sub = parser.add_subparsers(dest="cmd")

    # CSV → SCBF
    c1 = sub.add_parser("csv_to_custom", help="Convert CSV to SCBF format")
    c1.add_argument("csv", help="Input CSV file")
    c1.add_argument("out", help="Output SCBF file")
    c1.set_defaults(func=csv_to_custom_cmd)

    # SCBF → CSV
    c2 = sub.add_parser("custom_to_csv", help="Convert SCBF to CSV")
    c2.add_argument("file", help="SCBF file")
    c2.add_argument("out", help="Output CSV file")
    c2.add_argument("--cols", nargs="*", help="Optional subset of columns")
    c2.set_defaults(func=custom_to_csv_cmd)

    args = parser.parse_args()
    if not hasattr(args, "func"):
        parser.print_help()
    else:
        args.func(args)

if __name__ == "__main__":
    main()
