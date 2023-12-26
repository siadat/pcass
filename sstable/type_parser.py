import construct
import sstable.varint
import sstable.utils
import sstable.string_encoded
import sstable.sstable_decimal
import sstable.uuid
from lark import Lark, Transformer, v_args

text_cell_value = construct.Struct(
    # https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/rows/BufferCell.java#L272
    "cell_value_len" / sstable.varint.VarInt(),
    "cell_value" / sstable.string_encoded.StringEncoded(construct.Bytes(construct.this.cell_value_len), "utf-8"),
)
sstable.utils.assert_equal(b"\x04\x61\x62\x63\x64", text_cell_value.build({"cell_value_len": 4, "cell_value": "abcd"}))

int_cell_value = construct.Struct(
    "cell_value" / construct.Int32sb,
)
sstable.utils.assert_equal(b"\x00\x00\x00\x04", int_cell_value.build({"cell_value": 4}))

# The IntegerType is used for CQL sstable.varint type
# It is a Java BigInteger https://sourcegraph.com/github.com/apache/cassandra@cassandra-3.0.0/-/blob/src/java/org/apache/cassandra/serializers/IntegerSerializer.java?L35
integer_cell_value = construct.Struct(
    "length" / sstable.varint.VarInt(),
    "cell_value" / construct.BytesInteger(construct.this.length),
)
sstable.utils.assert_equal(b"\x01\x09", integer_cell_value.build({"length": 1, "cell_value": 9}))

# https://sourcegraph.com/github.com/apache/cassandra@cassandra-3.0.0/-/blob/src/java/org/apache/cassandra/db/marshal/ShortType.java?L54
# https://docs.oracle.com/javase/tutorial/java/nutsandbolts/datatypes.html
short_cell_value = construct.Struct(
    "length" / sstable.varint.VarInt(), # TODO: now sure why short needs a length? it should always be 2?
    "cell_value" / construct.BytesInteger(construct.this.length), # I think this is probably always 2 bytes, i.e. construct.Int16sb
)
sstable.utils.assert_equal(b"\x02\x00\x04", short_cell_value.build({"length": 2, "cell_value": 4}))

long_cell_value = construct.Struct(
    "cell_value" / construct.Int64sb,
)
sstable.utils.assert_equal(b"\x00\x00\x00\x00\x00\x00\x00\x04", long_cell_value.build({"cell_value": 4}))

# https://github.com/openjdk/jdk/blob/jdk8-b120/jdk/src/share/classes/java/math/BigInteger.java#L3697-L3726
# https://sourcegraph.com/github.com/apache/cassandra@cassandra-3.0.0/-/blob/src/java/org/apache/cassandra/serializers/DecimalSerializer.java?L45-59
# Note that this ^ serialize() method doesn't include the length of the value.
decimal_cell_value = construct.Struct(
    "cell_value" / sstable.sstable_decimal.DecimalNumber,
)

# Not tested with Cassnadra:
float_cell_value = construct.Struct(
    "cell_value" / construct.Float32b,
)
# not verified with Cassandra:
sstable.utils.assert_equal(b"\x00\x00\x00\x00", float_cell_value.build({"cell_value": 0}))
sstable.utils.assert_equal(b"\x40\x40\x00\x00", float_cell_value.build({"cell_value": 3.0}))

double_cell_value = construct.Struct(
    "cell_value" / construct.Float64b,
)
# not verified with Cassandra:
sstable.utils.assert_equal(b"\x00\x00\x00\x00\x00\x00\x00\x00", double_cell_value.build({"cell_value": 0}))
sstable.utils.assert_equal(b"\x40\x08\x00\x00\x00\x00\x00\x00", double_cell_value.build({"cell_value": 3.0}))


# Not tested with Cassnadra:
ascii_cell_value = construct.Struct(
    "length" / sstable.varint.VarInt(),
    "cell_value" / sstable.string_encoded.StringEncoded(construct.Bytes(construct.this.length), "ascii"),
)
sstable.utils.assert_equal(b"\x04\x61\x62\x63\x64", ascii_cell_value.build({"length": 4, "cell_value": "abcd"}))

bytes_cell_value = construct.Struct(
    "length" / sstable.varint.VarInt(),
    "cell_value" / construct.Bytes(construct.this.length),
)
sstable.utils.assert_equal(b"\x04\x61\x62\x63\x64", bytes_cell_value.build({"length": 4, "cell_value": b"abcd"}))

# The ByteType seems to be used for tinyint AND it has a length! WTF :shrug:
byte_cell_value = construct.Struct(
    "length" / sstable.varint.VarInt(),
    "cell_value" / construct.Bytes(construct.this.length),
)

# Not tested with Cassnadra:
boolean_cell_value = construct.Struct(
    "cell_value" / construct.OneOf(construct.Byte, [0, 1]),
)
sstable.utils.assert_equal(b"\x00", boolean_cell_value.build({"cell_value": False}))
sstable.utils.assert_equal(False, boolean_cell_value.parse(b"\x00").cell_value)

# https://sourcegraph.com/github.com/apache/cassandra@cassandra-3.0.0/-/blob/src/java/org/apache/cassandra/serializers/TimestampSerializer.java?L122
# Note that getTime() returns a Java `long` and it represents milliseconds
timestamp_cell_value = construct.Struct(
    "cell_value" / construct.Int64sb,
)
sstable.utils.assert_equal(b"\x00\x00\x00\x00\x00\x00\x00\x04", timestamp_cell_value.build({"cell_value": 4}))
sstable.utils.assert_equal(4, timestamp_cell_value.parse(b"\x00\x00\x00\x00\x00\x00\x00\x04").cell_value)

uuid_cell_value = construct.Struct(
    "cell_value" / sstable.uuid.Uuid,
)
java_type_to_construct = {
    # Sources:
    # - https://sourcegraph.com/github.com/apache/cassandra@cassandra-3.0.29/-/tree/src/java/org/apache/cassandra/db/marshal
    # - https://cassandra.apache.org/doc/stable/cassandra/cql/types.html
    "org.apache.cassandra.db.marshal.UTF8Type": text_cell_value,
    "org.apache.cassandra.db.marshal.ShortType": short_cell_value,
    "org.apache.cassandra.db.marshal.IntegerType": integer_cell_value,
    "org.apache.cassandra.db.marshal.Int32Type": int_cell_value,
    "org.apache.cassandra.db.marshal.LongType": long_cell_value,
    "org.apache.cassandra.db.marshal.DecimalType": decimal_cell_value,
    "org.apache.cassandra.db.marshal.AsciiType": ascii_cell_value,
    "org.apache.cassandra.db.marshal.ByteType": byte_cell_value,
    "org.apache.cassandra.db.marshal.BytesType": bytes_cell_value,
    "org.apache.cassandra.db.marshal.BooleanType": boolean_cell_value,
    "org.apache.cassandra.db.marshal.FloatType": float_cell_value,
    "org.apache.cassandra.db.marshal.DoubleType": double_cell_value,
    "org.apache.cassandra.db.marshal.TimestampType": timestamp_cell_value,
    "org.apache.cassandra.db.marshal.UUIDType": uuid_cell_value,
}

type_grammar = """
    ?start: type

    type: user_type | collection_type | basic_type
    collection_type: PACKAGE "(" [type ("," type)*] ")"
    user_type: USER_TYPE_PACKAGE "(" KEYSPACE_NAME "," type_name_hex ("," field_name_hex ":" type)+ ")"
    basic_type: PACKAGE

    PACKAGE: /[a-zA-Z_][a-zA-Z0-9_.]*/
    USER_TYPE_PACKAGE: "org.apache.cassandra.db.marshal.UserType"
    KEYSPACE_NAME: /[a-zA-Z_][a-zA-Z0-9_]*/
    type_name_hex:  HEX_STRING
    field_name_hex: HEX_STRING
    HEX_STRING: /[a-fA-F0-9]+/

    %import common.WS
    %ignore WS
"""


def hex_to_ascii(hex_string):
    """Converts a hexadecimal string to its ASCII representation."""
    try:
        ascii_string = bytearray.fromhex(hex_string).decode()
        return ascii_string
    except ValueError:
        return "Invalid Hex String"

assert "example_user_type2" == hex_to_ascii("6578616d706c655f757365725f7479706532")

class TypeTransformer(Transformer):
    @v_args(inline=True)
    def type(self, typ):
        return typ

    @v_args(inline=True)
    def basic_type(self, package):
        return java_type_to_construct[package]

    @v_args(inline=True)
    def user_type(self, user_type_package, keyspace_name, type_name_hex, *field_defs):
        type_name = hex_to_ascii(type_name_hex.children[0].value)
        return f"user_type: ks={keyspace_name} type_name={type_name} field_defs={field_defs}"

    @v_args(inline=True)
    def collection_type(self, package, *types):
        if package == "org.apache.cassandra.db.marshal.ListType":
            subcon = types[0]
            return make_list_struct(subcon)
        elif package == "org.apache.cassandra.db.marshal.SetType":
            subcon = types[0]
            return make_set_struct(subcon)
        elif package == "org.apache.cassandra.db.marshal.MapType":
            key_subcon = types[0]
            val_subcon = types[1]
            return make_map_struct(key_subcon, val_subcon)
        else:
            raise Exception(f"Unknown type: {package}")

def make_list_struct(subcon):
    return construct.Struct(
        "cell_path_length" / sstable.varint.VarInt(), # 16
        "cell_path" / sstable.uuid.Uuid,
        "cell_value_len" / sstable.varint.VarInt(),
        "cell_value" / subcon,
    )

def make_map_struct(key_subcon, val_subcon):
    return construct.Struct(
        # cell_path is key for MapType s
        "cell_key_len" / sstable.varint.VarInt(),
        "cell_key" / key_subcon,

        "cell_val_len" / sstable.varint.VarInt(),
        "cell_val" / key_subcon,
    )

def make_set_struct(val_subcon):
    return construct.Struct(
        "cell_val_len" / sstable.varint.VarInt(),
        "cell_val" / val_subcon,
    )
# Example usage
parser = Lark(type_grammar, parser='lalr', transformer=TypeTransformer())

# Testing with an example string
sstable.utils.assert_equal(int_cell_value, parser.parse('org.apache.cassandra.db.marshal.Int32Type'))

tests = [
    {
        "type": 'org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type)',
        "bytes": bytes([16])
            + b"\x00" * 16
            + b"\x04"
            + b"\x00\x00\x00\x7b",
        "obj": {
            "cell_path_length": 16,
            "cell_path": b"\x00" * 16,
            "cell_value_len": 4,
            "cell_value": {
                "cell_value": 123,
            },
        },
    },
    {
        "type": 'org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.Int32Type, org.apache.cassandra.db.marshal.Int32Type)',
        "bytes": b"\x04"
               + b"\x00\x00\x00\x0a"
               + b"\x04"
               + b"\x00\x00\x00\x14",
        "obj": {
            "cell_key_len": 4,
            "cell_key": {
                "cell_value": 10,
            },
            "cell_val_len": 4,
            "cell_val": {
                "cell_value": 20,
            },
        },
    },
    {
        "type": 'org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.Int32Type)',
        "bytes": b"\x04"
               + b"\x00\x00\x00\x0a",
        "obj": {
            "cell_val_len": 4,
            "cell_val": {
                "cell_value": 10,
            },
        },
    },
]

for test_case in tests:
    sstable.utils.assert_equal(test_case["bytes"], parser.parse(test_case["type"]).build(test_case["obj"]))
    sstable.utils.assert_equal(test_case["obj"], parser.parse(test_case["type"]).parse(test_case["bytes"]))

# TODO: sstable.utils.assert_equal('map-from-org.apache.cassandra.db.marshal.Int32Type-to-org.apache.cassandra.db.marshal.Int32Type', parser.parse('org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.Int32Type,org.apache.cassandra.db.marshal.Int32Type)'))
# TODO: sstable.utils.assert_equal(None, parser.parse('org.apache.cassandra.db.marshal.UserType(sina_test,74616773,74616773:org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type))'))
# TODO: sstable.utils.assert_equal(None, parser.parse('org.apache.cassandra.db.marshal.UserType(sina_test,62616e645f696e666f5f74797065,666f756e646564:org.apache.cassandra.db.marshal.IntegerType,6d656d62657273:org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.UTF8Type),6465736372697074696f6e:org.apache.cassandra.db.marshal.UTF8Type)'))
# TODO: sstable.utils.assert_equal('org.apache.cassandra.db.marshal.UserType(sina_test,6578616d706c655f757365725f74797065,6e616d65:org.apache.cassandra.db.marshal.UTF8Type,616765:org.apache.cassandra.db.marshal.Int32Type,6d65746164617461:org.apache.cassandra.db.marshal.UserType(sina_test,6578616d706c655f757365725f7479706532,686569676874:org.apache.cassandra.db.marshal.FloatType))')
# TODO: sstable.utils.assert_equal('org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.UserType(sina_test,6578616d706c655f757365725f74797065,6e616d65:org.apache.cassandra.db.marshal.UTF8Type,616765:org.apache.cassandra.db.marshal.Int32Type,6d65746164617461:org.apache.cassandra.db.marshal.UserType(sina_test,6578616d706c655f757365725f7479706532,686569676874:org.apache.cassandra.db.marshal.FloatType))))')
# TODO: sstable.utils.assert_equal('org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type))')
# TODO: sstable.utils.assert_equal('org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type)')
# TODO: sstable.utils.assert_equal('org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.UserType(sina_test,6578616d706c655f757365725f74797065,6e616d65:org.apache.cassandra.db.marshal.UTF8Type,616765:org.apache.cassandra.db.marshal.Int32Type,6d65746164617461:org.apache.cassandra.db.marshal.UserType(sina_test,6578616d706c655f757365725f7479706532,686569676874:org.apache.cassandra.db.marshal.FloatType)))')
# TODO: sstable.utils.assert_equal('org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.Int32Type,org.apache.cassandra.db.marshal.Int32Type)')
# TODO: sstable.utils.assert_equal('org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.Int32Type)')
