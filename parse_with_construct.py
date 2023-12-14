import io
import os
import argparse
import traceback

import construct

import sstable.sstable_data
import sstable.sstable_statistics
import sstable.positioned_construct

def parse_statistics_db(statistics_file):
    parsed = None
    sstable.positioned_construct.init()
    with open(statistics_file, "rb") as f:
        err = None
        err_pos = None
        err_traceback = None

        try:
            parsed = sstable.sstable_statistics.statistics_format.parse_stream(f)
            print("# Parsed Statistics.db:")
            print(parsed)
        except Exception as e:
            err = e
            err_pos = f.tell()
            err_traceback = traceback.format_exc()
        last_pos = f.tell()

    with open(statistics_file, "rb") as f:
        print()
        print("# Hex Statistics.db")
        sstable.positioned_construct.pretty_hexdump(statistics_file, f, last_pos, os.sys.stdout, err, err_pos, err_traceback, index=False)
    return parsed

def parse_data_db(data_file, parsed_statistics):
    parsed = None
    sstable.positioned_construct.init()
    with open(data_file, "rb") as f:
        err = None
        err_pos = None
        err_traceback = None

        try:
            parsed = sstable.sstable_data.data_format.parse_stream(f, sstable_statistics=parsed_statistics)
            print("# Parsed Data.db:")
            print(parsed)
        except Exception as e:
            err = e
            err_pos = f.tell()
            err_traceback = traceback.format_exc()
        last_pos = f.tell()

    with open(data_file, "rb") as f:
        print()
        print("# Hex Data.db")
        sstable.positioned_construct.pretty_hexdump(data_file, f, last_pos, os.sys.stdout, err, err_pos, err_traceback)
    return parsed

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('dir', type=str)
    args = parser.parse_args()

    parsed_statistics = parse_statistics_db(os.path.join(args.dir, "me-1-big-Statistics.db"))
    print("")
    parsed_data = parse_data_db(os.path.join(args.dir, "me-1-big-Data.db"), parsed_statistics)

    # # inspect:
    # for (k, v) in sstable.positioned_construct.global_position_map.items():
    #     print(k, v)

main()
