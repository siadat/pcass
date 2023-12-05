import utils

import construct

global_position_map = {}

def wrap_func(cls):
    original_read = cls._parse
    
    def new_parse(self, stream, context, path):
        start_pos = stream.tell()
        ret = original_read(self, stream, context, path)
        end_pos = stream.tell()
        global global_position_map
        global_position_map[(start_pos, end_pos)] = path
        return ret
    
    cls._parse = new_parse


def get_matches_for_pos(pos):
    global global_position_map
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


def parse(filepath):
    pass

def pretty_hexdump(filepath, last_parsed_pos, output_stream, err=None, err_pos=None, err_traceback=None, index=False):
    with open(filepath, "rb") as f:
        byts = f.read()
        for i, byte in enumerate(byts[:last_parsed_pos]):
            if index or err:
                output_stream.write(str(i) + "\t")
            output_stream.write(f"{utils.byte_repr(byte)} {get_matches_for_pos(i)}\n")
        if err:
            output_stream.write(f"Error while trying to parse position {err_pos}: {err}\n")
        output_stream.write("# Parsed to here.\n")
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
    global_position_map.clear()
    for cls in construct.Construct.__subclasses__():
        wrap_func(cls)
