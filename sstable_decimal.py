from decimal import Decimal
import construct

import varint
import utils

# Source https://github.com/openjdk/jdk/blob/jdk8-b120/jdk/src/share/classes/java/math/BigInteger.java#L3697-L3726
def to_byte_array(value):
    # Calculate the byte length, adding one for the sign bit
    byte_len = (value.bit_length() + 8) // 8
    # Convert to bytes
    byte_array = value.to_bytes(byte_len, byteorder='big', signed=True)
    # Remove leading zero bytes except for the number zero itself
    if value != 0:
        byte_array = byte_array.lstrip(b'\x00')
    return byte_array

def get_scale(number):
    # Convert the number to Decimal for high precision
    d = Decimal(str(number))

    # Determine an appropriate precision
    # Use the exponent from the scientific notation to set the precision
    _, exp = d.as_tuple().exponent, abs(d.as_tuple().exponent)
    precision = max(50, exp + 5)  # Adding some buffer to the precision

    # Convert to string using the dynamic precision format
    str_number = format(d, f'.{precision}f')

    # Split the string at the decimal point
    parts = str_number.split('.')
    
    # Count the number of digits in the fractional part, excluding trailing zeros
    if len(parts) == 2:
        return len(parts[1].rstrip('0'))
    else:
        return 0

def from_byte_array(value):
    return [0x01, 0x02, 0x03]

utils.assert_equal(2, get_scale(123.45))
utils.assert_equal(2, get_scale(-123.45))
utils.assert_equal(0, get_scale(0))
utils.assert_equal(0, get_scale(1230))
utils.assert_equal(0, get_scale(-1230))
utils.assert_equal(14, get_scale(1e-14))

class _DecimalAdapter(construct.Adapter):
    def _decode(self, obj, context, path):
        return obj.unscaled_big_int * (10**-obj.scale)

    def _encode(self, obj, context, path):
        scale = get_scale(obj)
        scale_bytes = scale.to_bytes(4, byteorder='big')
        value_bytes = to_byte_array(int(obj*(10**scale)))
        return {
            "total_length": len(value_bytes) + len(scale_bytes),
            "scale": int(scale),
            "unscaled_big_int": int(obj*(10**scale)),
        }

test_cases = [
    { "bytes": b"\x00", "number": 0 },
    { "bytes": b"\x01", "number": 1 },
    { "bytes": b"\xff", "number": 0xff },
    { "bytes": b"\xff\xff", "number": 0xffff },
    { "bytes": b"\xff", "number": -1 },
    { "bytes": b"\xfe", "number": -2 },
    { "bytes": b"\xff\x01", "number": -0xff },
]

for tc in test_cases:
    utils.assert_equal(tc["bytes"], to_byte_array(tc["number"]))

DecimalNumber = _DecimalAdapter(construct.Struct(
    "total_length" / varint.VarInt(),
    "scale" / construct.Int32ub, # scale as in (unscaled_big_int * 10**scale)
    "unscaled_big_int" / construct.BytesInteger(construct.this.total_length-4), # 4 is the length of Int32ub for scale
))

utils.assert_equal(b"\x05\x00\x00\x00\x0e\x01", DecimalNumber.build(0.00000000000001))
utils.assert_equal(0.00000000000001, DecimalNumber.parse(b"\x05\x00\x00\x00\x0e\x01"))
