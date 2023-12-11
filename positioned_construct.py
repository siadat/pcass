import os
import utils

import construct

global_position_map = {}
all_keys = []

def wrap_func(cls):
    original_read = cls._parse
    debug = False
    
    def new_parse(self, stream, context, path):
        start_pos = stream.tell()

        if debug:
            parent_depth = 0
            if hasattr(context, "_"):
                parent_depth = context._.depth
            context.depth = parent_depth + 1
            print("    " * context.depth, f"+ {self.__class__} start_pos={start_pos} {path}")

        ret = original_read(self, stream, context, path)
        end_pos = stream.tell()

        if debug:
            print("    " * context.depth, f"- {self.__class__} end_pos={end_pos} {path}")
            context.depth -= 1

        global global_position_map
        current = global_position_map.get((start_pos, end_pos), [])
        current.append(path) # + " " + self.__class__.__name__
        global_position_map[(start_pos, end_pos)] = current
        return ret
    
    cls._parse = new_parse


def global_position_map_keys():
    global global_position_map
    global all_keys

    if all_keys:
        return all_keys
    # this will sort them by (+start position) and (-end position):
    all_keys = sorted(global_position_map.keys(), key=lambda key: (key[0], -key[1]))
    return all_keys


def get_matches_for_pos(pos):
    global global_position_map
    result = []
    for (start, end) in global_position_map_keys():
        if pos > end:
            continue
        if pos < start:
            continue
        # The reason for "< end" instead of "<= end" is that the end pos is recorded after a _read is returned, so pos is one ahead.
        if start <= pos < end:
            result.append(global_position_map[(start, end)])

    if result:
        return result[-1][0]
    else:
        return None
    # return " >> ".join(result)


def parse(filepath):
    pass

def pretty_hexdump(filepath, input_stream, last_parsed_pos, output_stream, err=None, err_pos=None, err_traceback=None, index=False):
    byts = input_stream.read()
    for i, byte in enumerate(byts[:last_parsed_pos]):
        if index or err:
            output_stream.write(str(i) + "\t")
        output_stream.write(f"{utils.byte_repr(byte)} {get_matches_for_pos(i)}\n")
    if err:
        output_stream.write(f"\nError at position {err_pos}: {err}\n")
        output_stream.write(f"\nTrace: {err_traceback}\n")
        output_stream.write(f"\n")
    output_stream.write(f"\n")
    output_stream.write(f"# Successfully parsed {os.path.basename(filepath)} to here.\n")
    output_stream.write(f"\n")
    for i, byte in enumerate(byts[last_parsed_pos:]):
        if index or err:
            output_stream.write(str(i+last_parsed_pos) + "\t")
        output_stream.write(utils.byte_repr(byte) + "\n")

# Make sure init is called *after* you have imported or defined your Construct
# format objects. Otherwise, your won't get nice positioned paths for them. You
# can see what I mean by calling init() before you define your Construct
# structs and check the output of pretty_hexdump.
def init():
    global global_position_map
    global all_keys
    global_position_map.clear()
    all_keys.clear()
    for cls in construct.Construct.__subclasses__():
        wrap_func(cls)
