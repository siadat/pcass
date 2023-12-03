import construct
import io

def parse(stream):
    first_byte = stream.read(1)[0]
    if not first_byte & 0b10000000:
        return first_byte

    # count the number of 1s in first_byte:
    ones_count = 0
    zeros_count = 0
    zero_padding = 0
    first_value_bits = []
    # import pdb; pdb.set_trace()

    for i in range(8):
        index = 7 - i
        masked_value = first_byte & (1<<index)
        if masked_value > 0:
            ones_count += 1
        else:
            break

    for i in range(ones_count, 8):
        index = 7 - i
        masked_value = first_byte & (1<<index)
        if masked_value == 0:
            zeros_count += 1
        else:
            break

    for i in range(ones_count+zeros_count, 8):
        index = 7 - i
        masked_value = first_byte & (1<<index)
        if masked_value == 0:
            first_value_bits.append(0)
        else:
            first_value_bits.append(1)

    first_value_byte = [0] * (8-len(first_value_bits)) + first_value_bits
    first_value_byte = int(''.join(map(str, first_value_byte)), 2)

    byts = bytes([first_value_byte]) + stream.read(ones_count)
    # print(ones_count, first_value_bits, first_value_byte, list(map(lambda b: bin(b), byts)))
    return int.from_bytes(byts, 'big')

assert parse(io.BytesIO(bytes([0b00000000]))) == 0
assert parse(io.BytesIO(bytes([0b00000001]))) == 1
assert parse(io.BytesIO(bytes([0b01111111]))) == 127
assert parse(io.BytesIO(bytes([0b10000000, 0b10000000]))) == 128
assert parse(io.BytesIO(bytes([0b10000000, 0b10000001]))) == 129
assert parse(io.BytesIO(bytes([0b10000010, 0b10000000]))) == 640
assert parse(io.BytesIO(bytes([0b11000000, 0b10000000, 0b00000101]))) == 32773

class VarInt(construct.Construct):
    def _parse(self, stream, context, path):
        return parse(stream)


    def _build(self, obj, stream, context, path):
        print(f"_build {stream}")
        # write obj to the stream
        # return same value (obj) or a modified value
        # that will replace the context dictionary entry
        pass

    def _sizeof(self, context, path):
        print(f"_sizeof {context} {path}")
        # return computed size (when fixed size or depends on context)
        # or raise SizeofError (when variable size or unknown)
        pass

