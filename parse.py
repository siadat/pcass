import argparse 
import string
import yaml

from kaitaistruct import KaitaiStruct
import sstable

class YamlDumper(yaml.Dumper):
    def represent_binary(self, data):
        value = ' '.join(data[i:i+1].hex() for i in range(0, len(data), 1))
        return self.represent_scalar(u'tag:yaml.org,2002:str', value)

yaml.add_representer(bytes, YamlDumper.represent_binary, Dumper=YamlDumper)


def object_to_dict(obj, seen=None):
    if seen is None:
        seen = set()
    
    obj_id = id(obj)
    # if obj_id in seen:
    #     return f'<Circular reference detected: {obj}>'
    seen.add(obj_id)
    
    if not hasattr(obj, "__dict__"):
        return obj
    
    result = {}
    for key, val in obj.__dict__.items():
        if key.startswith("_") or key.startswith("zzz"):
            continue
        if isinstance(val, list):
            element = []
            for item in val:
                element.append(object_to_dict(item, seen))
            result[key] = element
        elif isinstance(val, dict):
            element = {}
            for k, v in val.items():
                element[k] = object_to_dict(v, seen)
            result[key] = element
        else:
            result[key] = object_to_dict(val, seen)
    
    return result


def byte_repr(byte):
    if 32 <= byte <= 126:
        s = bytes([byte])
        s = repr(s.decode("utf-8"))
    else:
        s = '───'
    binary = format(byte, '08b')
    return f"0x{byte:02x}\tb{binary}\t{byte:>3}\t{s}"

global_position_map = {}

def get_matches_for_pos(pos):
    # this will sort them by (+start position) and (-end position):
    all_keys = sorted(global_position_map.keys(), key=lambda key: (key[0], -key[1]))

    result = []
    for (start, end) in all_keys:
        # The reason for "< end" instead of "<= end" is that the end pos is recorded after a _read is returned, so pos is one ahead.
        if start <= pos < end:
            result.append(global_position_map[(start, end)])
    return " > ".join(result)

def wrap_read(cls):
    # Save the original _read method
    original_read = cls._read
    
    # Define a new _read method that includes tracking
    def new_read(self):
        start_pos = self._io.pos()
        original_read(self)  # Call the original _read method
        end_pos = self._io.pos()
        # Update the global map with the position range and the name of the class
        global_position_map[(start_pos, end_pos)] = self.__class__.__name__
    
    # Replace the original _read with the new_read method
    cls._read = new_read

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('file', type=str)
    args = parser.parse_args()

    for cls in KaitaiStruct.__subclasses__():
        wrap_read(cls)

    g = sstable.Sstable.from_file(args.file)
    print(yaml.dump(object_to_dict(g), Dumper=YamlDumper))

    # from IPython import embed; embed()
    # import pdb; pdb.set_trace()


    stream = g._io
    pos = stream.pos()
    # for key in sorted(global_position_map.keys(), key=lambda key: (key[0], -key[1])):
    #     print(">>", key)
    with open(args.file, "rb") as f:
        byts = f.read() # stream.read_bytes_full()
        print("# Bytes:")
        for i, byte in enumerate(byts[:pos]):
            # print("0x..")
            print(byte_repr(byte), get_matches_for_pos(i))
        print(f"# Parsed to here.")
        for byte in byts[pos:]:
            print(byte_repr(byte))
        # parsed_hex_string = ' '.join(f'{byte:02x}' for byte in byts[:pos])
        # remaining_hex_string = ' '.join(f'{byte:02x}' for byte in byts[pos:])
        # print(f"Bytes:", parsed_hex_string, f"-- (pos {pos}) --", remaining_hex_string)

    # print("g.partition.partition_header.key:", g.partition.partition_header.key)
    # print("g.partition.partition_header.deletion_time.local_deletion_time:", g.partition.partition_header.deletion_time.local_deletion_time)
    # print("g.partition.partition_header.deletion_time.marked_for_delete_at:", g.partition.partition_header.deletion_time.marked_for_delete_at)
    # print("g.partition.partition_header.deletion_time.marked_for_delete_at:", g.partition.row)

main()
