import io
import argparse 
import traceback

import construct

import sstable_construct
import positioned_construct

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('file', type=str)
    args = parser.parse_args()

    positioned_construct.init()

    last_pos = 0
    with open(args.file, "rb") as f:
        err = None
        err_pos = None
        err_traceback = None

        try:
            parsed = sstable_construct.format.parse_stream(f)
            print(parsed)
        except Exception as e:
            err = e
            err_pos = f.tell()
            err_traceback = traceback.format_exc()
        # import pdb; pdb.set_trace()
        last_pos = f.tell()

    string_buffer = io.StringIO()
    positioned_construct.pretty_hexdump(args.file, last_pos, string_buffer, err, err_pos, err_traceback)
    print(string_buffer.getvalue(), end="")

main()
