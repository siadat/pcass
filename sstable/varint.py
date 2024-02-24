import textwrap
import construct
import io
import sstable.utils

def parse(stream):
    first_byte = stream.read(1)[0]
    if not first_byte & 0b10000000:
        return first_byte

    # count the number of 1s in first_byte:
    ones_count = 0
    zeros_count = 0
    zero_padding = 0
    first_value_bits = []

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

def build(value):
    # values that are a single byte and don't start with a 1 don't need any
    # encoding, and decoders also know that because it starts with a 0.
    # value = value + 123
    if value < 0b10000000:
        return bytes([value])

    # get the number of bits needed to represent this value:
    bit_length = value.bit_length()
    byte_length = -(-bit_length // 8)
    first_byte = (value >> (8*(byte_length-1))).to_bytes(1, byteorder="big")[0]

    if byte_length > 8:
        raise f"Only 8 byte integers are supported, {value} is {byte_length} bytes"

    flag_bits = 0
    for i in range(byte_length):
        flag_bits |= 1<<(7-i)

    if byte_length == 8:
        # No separating 0 is needed
        return bytes([flag_bits] + list(value.to_bytes(byte_length, byteorder="big")))

    if flag_bits & first_byte == 0:
        # flag_bits_mask and first_byte do not overlap, so we should merge flag_bits
        # and first_byte, but we need to need to remove one of the 1s from
        # flag_bits, because the number of bytes that follow the flag_bits is
        # one less than the number of bytes needed to represent the number.
        # Removing the right-most-1 is done with (byte&(byte-1)):
        flag_bits = flag_bits & (flag_bits-1)
        merged_first_byte = flag_bits | first_byte
        return bytes([merged_first_byte] + list(value.to_bytes(byte_length, byteorder="big"))[1:])
    else:
        # flag_bits_mask and first_byte overlap, so we need to start the value bits
        # from the next byte:
        return bytes([flag_bits] + list(value.to_bytes(byte_length, byteorder="big")))

class VarInt(construct.Construct):
    def _parse(self, stream, context, path):
        return parse(stream)


    def _build(self, obj, stream, context, path):
        # return same value (obj) or a modified value
        # that will replace the context dictionary entry
        stream.write(build(obj))
        return obj

    # def _sizeof(self, context, path):
    #     # return computed size (when fixed size or depends on context)
    #     # or raise SizeofError (when variable size or unknown)
    #     print(f"_sizeof {context} {path}")
