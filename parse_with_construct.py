import argparse 
import traceback

import construct

import sstable_construct

global_position_map = {}

def get_matches_for_pos(pos):
    # this will sort them by (+start position) and (-end position):
    all_keys = sorted(global_position_map.keys(), key=lambda key: (key[0], -key[1]))

    result = []
    for (start, end) in all_keys:
        # The reason for "< end" instead of "<= end" is that the end pos is recorded after a _read is returned, so pos is one ahead.
        if start <= pos < end:
            result.append(global_position_map[(start, end)])
    if result:
        return result[-1]
    else:
        return None
    # return " >> ".join(result)

def byte_repr(byte):
    if 32 <= byte <= 126:
        s = bytes([byte])
        s = repr(s.decode("utf-8"))
    else:
        s = '───'
    binary = format(byte, '08b')
    return f"0x{byte:02x}\tb{binary}\t{byte:>3}\t{s}"


def wrap_func(cls):
    original_read = cls._parse
    
    def new_parse(self, stream, context, path):
        start_pos = stream.tell()
        ret = original_read(self, stream, context, path)
        end_pos = stream.tell()
        global_position_map[(start_pos, end_pos)] = path
        return ret
    
    cls._parse = new_parse


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('file', type=str)
    args = parser.parse_args()

    for cls in construct.Construct.__subclasses__():
        wrap_func(cls)

    last_pos = 0
    with open(args.file, "rb") as f:
        err = None
        err_pos = None
        err_traceback = None

        # parsed = sstable_construct.format.parse_stream(f)
        # print(parsed)

        try:
            parsed = sstable_construct.format.parse_stream(f)
            print(parsed)
        except Exception as e:
            err = e
            err_pos = f.tell()
            err_traceback = traceback.format_exc()
        # import pdb; pdb.set_trace()
        last_pos = f.tell()

    with open(args.file, "rb") as f:
        byts = f.read()
        for i, byte in enumerate(byts[:last_pos]):
            if err:
                print(i, end='\t')
            print(byte_repr(byte), get_matches_for_pos(i))
        if err:
            print(f"Error while trying to parse position {err_pos}: {err}")
        print(f"# Parsed to here.")
        for i, byte in enumerate(byts[last_pos:]):
            if err:
                print(i+last_pos, end='\t')
            print(byte_repr(byte))

main()
