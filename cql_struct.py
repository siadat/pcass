import construct
import debug
import sstable.string_encoded
import sstable.utils
import sstable.greedy_range
import sstable.type_parser
import sstable.dynamic_switch

construct.setGlobalPrintFullStrings(sstable.utils.PRINT_FULL_STRING)

# This is used in the version field of the frame header to indicate that the
# frame is a response.
RESPONSE_FLAG = 0b10000000


short_int = construct.Int16ub

string = construct.Struct(
    "length" / short_int,
    "string" / sstable.string_encoded.StringEncoded(construct.Bytes(construct.this.length), "utf-8"),
)
long_string = construct.Struct(
    "length" / construct.Int32ub,
    "string" / sstable.string_encoded.StringEncoded(construct.Bytes(construct.this.length), "utf-8"),
)

string_list = construct.Struct(
    "count" / short_int,
    "strings" / construct.Array(construct.this.count, long_string),
)

class ResultKind:
    VOID          = 0x0001
    ROWS          = 0x0002
    SET_KEYSPACE  = 0x0003
    PREPARED      = 0x0004
    SCHEMA_CHANGE = 0x0005

result_void = construct.Struct(
    #"kind" / construct.Int32ub,
    "body" / construct.Bytes(0),
)

class ResultRowsFlags:
    GLOBAL_TABLES_SPEC = 0x0001
    HAS_MORE_PAGES     = 0x0002
    NO_METADATA        = 0x0004

option = construct.Struct(
    "id" / construct.Int16ub,
    # The reason we use dynamic_switch here instead of the standard Switch is
    # that this is a recursive struct, eg option is used for the type of
    # list, sets, etc.
    "value" / sstable.dynamic_switch.DynamicSwitch(
        lambda ctx: ctx.id,
        lambda id: {
            0x0000: string,
            0x0001: construct.Bytes(0), # ascii
            0x0002: construct.Bytes(0), # bigint
            0x0003: construct.Bytes(0), # blob
            0x0004: construct.Bytes(0), # boolean
            0x0005: construct.Bytes(0), # counter
            0x0006: construct.Bytes(0), # decimal
            0x0007: construct.Bytes(0), # double
            0x0008: construct.Bytes(0), # float
            0x0009: construct.Bytes(0), # int
            0x000B: construct.Bytes(0), # timestamp
            0x000C: construct.Bytes(0), # uuid
            0x000D: construct.Bytes(0), # varchar
            0x000E: construct.Bytes(0), # varint
            0x000F: construct.Bytes(0), # timeuuid
            0x0010: construct.Bytes(0), # inet
            0x0011: construct.Bytes(0), # date
            0x0012: construct.Bytes(0), # time
            0x0013: construct.Bytes(0), # smallint
            0x0014: construct.Bytes(0), # tinyint
            0x0020: option, # list
            0x0021: construct.Struct( # map
                "key" / option,
                "value" / option,
            ),
            0x0022: option, # set
            # TODO: 0x0030: construct.Bytes(0), # udt
            # TODO:  0x0031: construct.Bytes(0), # tuple
        }[id],
    ),
)

result_rows = construct.Struct(
    "metadata" / construct.Struct(
        "flags" / construct.Int32ub,
        "columns_count" / construct.Int32ub,
        "paging_state" / construct.If(lambda ctx: ctx.flags & ResultRowsFlags.HAS_MORE_PAGES, long_string),
        "global_table_spec" / construct.If(lambda ctx: (ctx.flags & ResultRowsFlags.GLOBAL_TABLES_SPEC) and (not ctx.flags & ResultRowsFlags.NO_METADATA), construct.Struct(
            "keyspace" / string,
            "table" / string,
        )),
        "column_specs" / construct.If(lambda ctx: not ctx.flags & ResultRowsFlags.NO_METADATA,
            construct.Array(construct.this.columns_count, construct.Struct(
                "keyspace" / construct.If(lambda ctx: not ctx._.flags & ResultRowsFlags.GLOBAL_TABLES_SPEC, string),
                "table" / construct.If(lambda ctx: not ctx._.flags & ResultRowsFlags.GLOBAL_TABLES_SPEC, string),
                "name" / string,
                "type" / option,
            ),
        )),
    ),
    "rows_count" / construct.Int32ub,
    "rows_content" / construct.Array(construct.this.rows_count, construct.Struct(
        "row" / construct.Array(construct.this._.metadata.columns_count, construct.Struct(
            "column_length" / construct.Int32sb,
            "column_value" / construct.If(lambda ctx: ctx.column_length>0, construct.Bytes(construct.this.column_length)),
        )),
    )),
)

class FrameFlags:
    COMPRESSION_FLAG = 0x01
    TRACING_FLAG     = 0x02
    CUSTOM_PAYLOAD   = 0x04
    WARNING_FLAG     = 0x08

class ErrorCode:
    SERVER_ERROR     = 0x0000
    PROTOCOL_ERROR   = 0x000A
    AUTH_ERROR       = 0x0100
    UNAVAILABLE      = 0x1000
    OVERLOADED       = 0x1001
    IS_BOOTSTRAPPING = 0x1002
    TRUNCATE_ERROR   = 0x1003
    WRITE_TIMEOUT    = 0x1100
    READ_TIMEOUT     = 0x1200
    READ_FAILURE     = 0x1300
    FUNCTION_FAILURE = 0x1400
    WRITE_FAILURE    = 0x1500
    SYNTAX_ERROR     = 0x2000
    UNAUTHORIZED     = 0x2100
    INVALID          = 0x2200
    CONFIG_ERROR     = 0x2300
    ALREADY_EXISTS   = 0x2400
    UNPREPARED       = 0x2500


# Opcode is an integer byte that distinguishes the actual message:
# https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v5.spec#L342-L358
class OpCode:
    ERROR          = 0x00
    STARTUP        = 0x01
    READY          = 0x02
    AUTHENTICATE   = 0x03
    OPTIONS        = 0x05
    SUPPORTED      = 0x06
    QUERY          = 0x07
    RESULT         = 0x08
    PREPARE        = 0x09
    EXECUTE        = 0x0A
    REGISTER       = 0x0B
    EVENT          = 0x0C
    BATCH          = 0x0D
    AUTH_CHALLENGE = 0x0E
    AUTH_RESPONSE  = 0x0F
    AUTH_SUCCESS   = 0x10

error = construct.Struct(
    "code" / construct.Int32ub,
    "length" / short_int,
    "message" / sstable.string_encoded.StringEncoded(construct.Bytes(construct.this.length), "utf-8"),
)
result = construct.Struct(
    "kind" / construct.Int32ub,
    "result" / construct.Switch(construct.this.kind, {
        ResultKind.VOID: construct.Bytes(0),
        ResultKind.ROWS: result_rows,
    }),
)

# https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L66
query = construct.Struct(
    "query" / long_string,
    "consistency" / short_int,
    "flags" / construct.Byte,
    # TODO: the rest of the body depends on the flags
)

frame = construct.Struct(
    # Version is not well documented in the protocol unfortunately.
    # The new client sends version 66 == 0x42 (MAX_SUPPORTED) with OpCodes.OPTIONS as the first every request sent by the client.
    # Sources:
    #   https://docs.datastax.com/en/developer/python-driver/3.29/api/cassandra/cluster/#:~:text=protocol_version%20%3D%2066
    #   https://docs.datastax.com/en/developer/python-driver/3.29/api/cassandra/#cassandra.ProtocolVersion:~:text=by%20this%20driver.-,MAX_SUPPORTED%20%3D%2066,-Maximum%20protocol%20version
    "version" / construct.Hex(construct.Byte),
    "flags" / construct.Hex(construct.Byte),
    "stream" / construct.Int16sb,
    "opcode" / construct.Hex(construct.Byte),
    "length" / construct.Int32ub,
    "body" / construct.Switch(construct.this.opcode, {
        OpCode.ERROR: error,
        OpCode.QUERY: query,
        OpCode.RESULT: result,
        # TODO? OpCode.READY: None,
    }),
)

string_list = construct.Struct(
    "count" / short_int,
    "strings" / construct.Array(construct.this.count, construct.Struct(
        "length" / short_int,
        "string" / sstable.string_encoded.StringEncoded(construct.Bytes(construct.this.length), "utf-8"),
    )),
)

string_map = construct.Struct(
    "count" / short_int,
    "keyvals" / construct.Array(construct.this.count, construct.Struct(
        "key_length" / short_int,
        "key" / sstable.string_encoded.StringEncoded(construct.Bytes(construct.this.key_length), "utf-8"),
        "val_length" / short_int,
        "val" / sstable.string_encoded.StringEncoded(construct.Bytes(construct.this.val_length), "utf-8"),
    )),
)

string_multimap = construct.Struct(
    "count" / short_int,
    "keyvals" / construct.Array(construct.this.count, construct.Struct(
        "length" / short_int,
        "key" / sstable.string_encoded.StringEncoded(construct.Bytes(construct.this.length), "utf-8"),
        "values" / string_list,
    )),
)

string_multimap_example = {
    "bytes": b'\x00\x01'
           + b'\x00\x04' + b'key1'
           + b'\x00\x02'
           + b'\x00\x06value1'
           + b'\x00\x06value2',
    "obj": {
        "count": 1,
        "keyvals": [
            {
                "length": 4,
                "key": "key1",
                "values": {
                    "count": 2,
                    "strings": [
                        {
                            "length": 6,
                            "string": "value1",
                        },
                        {
                            "length": 6,
                            "string": "value2",
                        },
                    ],
                },
            },
        ],
    },
}
sstable.utils.assert_equal(string_multimap_example["bytes"], string_multimap.build(string_multimap_example["obj"]))

error_example = {
    "bytes": b"\x00\x00\x00\x0A"  # code
           + b"\x00\x05" # length
           + b"hello", # message
    "obj": {
        "code": ErrorCode.PROTOCOL_ERROR,
        "length": 5,
        "message": "hello",
    },
}
sstable.utils.assert_equal(error_example["bytes"], error.build(error_example["obj"]))

frame_example = {
    "bytes": b"\x84" # version
           + b"\x00" # flags
           + b"\x00\x00" # stream
           + b"\x00" # opcode
           + len(error_example["bytes"]).to_bytes(4, byteorder='big')
           + error_example["bytes"],
    "obj": {
        "version": 0x04 | RESPONSE_FLAG,
        "flags": 0x00,
        "stream": 0x0000,
        "opcode": OpCode.ERROR,
        "length": len(error_example["bytes"]),
        "body": error_example["obj"],
    },
}
sstable.utils.assert_equal(frame_example["bytes"], frame.build(frame_example["obj"]))

invalid_query_reponse_example = frame.parse(sstable.utils.hexstring_to_bytes("""
    # original: 84000003000000002100002200001b756e636f6e66696775726564207461626c652070656572735f7632
    84 # version
    00 # flags
    0003 # stream
    00 # opcode
    # body:
    00 00 00 21 # length
    00 00 22 00 # code = Invalid: The query is syntactically correct but invalid
    00 1b # message length
    756e636f6e66696775726564207461626c652070656572735f7632 # = "unconfigured table peers_v2"
"""))
sstable.utils.assert_equal("unconfigured table peers_v2", invalid_query_reponse_example.body.message)

query_example = frame.parse(sstable.utils.hexstring_to_bytes("""
    # 0400000407000000330000002c53454c454354202a2046524f4d2073797374656d2e6c6f63616c205748455245206b65793d276c6f63616c27000100
    04000004 07 000000330000002c53454c454354202a2046524f4d2073797374656d2e6c6f63616c205748455245206b65793d276c6f63616c27000100
"""))
sstable.utils.assert_equal("SELECT * FROM system.local WHERE key='local'", query_example.body.query.string)
sstable.utils.assert_equal(1, query_example.body.consistency)
sstable.utils.assert_equal(0, query_example.body.flags)

got_example = frame.parse(sstable.utils.hexstring_to_bytes("""
    # 84000004080000198b000000020000000100000012000673797374656d00056c6f63616c00036b6579000d000c626f6f747374726170706564000d001162726f6164636173745f616464726573730010000c636c75737465725f6e616d65000d000b63716c5f76657273696f6e000d000b646174615f63656e746572000d0011676f737369705f67656e65726174696f6e00090007686f73745f6964000c000e6c697374656e5f61646472657373001000176e61746976655f70726f746f636f6c5f76657273696f6e000d000b706172746974696f6e6572000d00047261636b000d000f72656c656173655f76657273696f6e000d000b7270635f616464726573730010000e736368656d615f76657273696f6e000c000e7468726966745f76657273696f6e000d0006746f6b656e730022000d000c7472756e63617465645f61740021000c000300000001000000056c6f63616c00000009434f4d504c4554454400000004ac1100020000000c5465737420436c757374657200000005332e342e300000000b6461746163656e7465723100000004658b6c1d00000010f1f9ecf2ecae4f63968a61fba4b9574a00000004ac11000200000001340000002b6f72672e6170616368652e63617373616e6472612e6468742e4d75726d757233506172746974696f6e6572000000057261636b3100000006332e302e3239000000040000000000000010286d83bc098a392fbccf243455b0e0fe0000000632302e312e300000176900000100000000132d313132353834373632373936383737353637000000142d31313936343036373737333136303230333836000000132d313233373531353738323830333233343235000000142d31323734363039363933313839393132363834000000142d31333036393633373930353834373834353734000000142d31333830373535353134313231343738323835000000142d31343336323333393132393631323436363734000000142d31343433333238353230353339333433353932000000142d31353234363334393234353631323335313031000000142d31353237383632333039383837353431303735000000142d31353238373134323235373533383038323330000000142d31353830323532393435353833343637373130000000122d3136383932333239303035323939343335000000142d31383333323137353732353138353233303433000000142d31393131353133353833373730393232343131000000142d31393338343835303731323635343339353233000000142d32303832373637363230343634303430393534000000142d32313333363133373331373233343036333539000000142d32313336353538353133383031313933313531000000142d32313538313934313132353034383030353836000000142d32313631383637333439363130393832303733000000142d32333537323438323137383237383537323933000000142d32333738383538313535303231333334373636000000142d32343331333133343635393832323837343331000000132d323435333335393736313531383536383632000000132d323438343731383633303530353737333934000000132d323438383935393431343835383639323330000000142d32353238353239363938303633353633373434000000142d32363134363237333531343236333234303037000000142d32363732363037363336323536373339363134000000142d32363936303930333536393136373033323937000000142d32373239333434383332303031323735353630000000142d32383231393830323932373435393834363530000000142d32383733393635313435383932313039313234000000142d32383737333038353934303537353630333632000000142d32393838323339373034393834383736383939000000142d33313033343237373136323038313930303536000000132d333235323233383037303639313237343136000000142d33323935393239343135383635323435323833000000142d33343434373135343630393432393234383734000000142d33343736323837363532383636363837393639000000142d33353630313932333437343739323839323334000000142d33363735363131363531303336353835353831000000142d33363934303835333236393939343432393839000000142d33373733343931383236343232323837323235000000142d33373735393634343938353138313332393639000000142d33373736363336313734333038303336383331000000142d33383031383630313438343838393431303331000000142d33383335333037383130393038383734393739000000142d33393035303332393234383137333430343035000000142d33393035353239353737373231333037373138000000142d33393731333339353232303031393739383436000000142d34303031373434393132313532363837373632000000142d34313135383639393635303037303437313534000000142d34313736343633313230363834343839393038000000142d34323836393233353430313134323539303737000000142d34343036323830303636373837313137393533000000142d34353039323739373938383330333836323132000000142d34353432373832313932333136303430383936000000142d34353638363637393333393031393531333930000000142d34363035373534323734323930373738303134000000142d34363734353434363730363130393030323933000000132d343639363732373539303036373330333836000000132d343834323737313039313936383836343430000000142d34383532333838393232393030393238333033000000142d35303932383639313337303835383139323032000000142d35333436353034313338343233343331323730000000142d35333635363230383434303038393637343639000000142d35353637343531393432333232383038333230000000142d35363039303333373638353134373733363538000000142d35363939333435393637383132353731353337000000122d3537303531303636393136353038363638000000142d35373236393530343430303236353539343432000000142d35373937313335393039303535373039363436000000142d35383636363538343037323730343735363132000000132d353838303934343932343834343639363135000000142d35383835393731353431373335313236353436000000132d353839313338343333323836383530363433000000142d35393036323036373833373537323632303433000000142d35393236383235363833393039303939303732000000142d35393730393437343237343535373632303937000000142d35393833343338313932303635333830383239000000142d36303637353333383437353239353636343838000000132d363135313633363133383631333837383835000000132d363138313030323533373231313939383032000000142d36323134383334323332373834373233383031000000142d36323634343830383138353731313535313333000000142d36323837353438303331383030383437383832000000132d363336363733333833373330303532343338000000142d36343033393131373431323139363033393937000000142d36343132333433353938323636383539313936000000142d36343731373037363739393231333238303734000000142d36363136313334323939363931363737323332000000142d36363239383136383533303830303238383735000000132d363739393831353730363334353032303937000000142d36383336303131353034323431343138383234000000142d36383733343937363335343139323237333530000000142d36383832393238343238313632383939353930000000142d36393634353034303938383631363433363438000000132d373134373834343137313432393938343731000000142d37313735303331353534373935303336353637000000142d37323932333033313432333337333631323135000000142d37333331373532363737363932393934343034000000142d37333730333533383238313838323234303137000000142d37353536313635383538303936383237323937000000142d37363436393037383832333638333839303933000000142d37363739323732333730333537393730383737000000142d37363833313532343131303036323736363037000000142d37363833373630313035333835333035313638000000142d37373733313634343630393933393634393533000000142d37373739373939333736383730313535343633000000142d37383133303739363430393433333130373539000000142d37393232393435353532393334313834383631000000142d37393531353537333437313335333132373636000000142d37393534363635303539353931363535353832000000132d383031333336343530303930353335333834000000142d38303331343030353133303939373038323230000000142d38303434313135363831303331303933373635000000142d38303539313639383735343734343836363536000000142d38313235313333383434353938363830303932000000142d38313936383333303231343535333939363330000000142d38333632363135383234353937353038303235000000142d38343938303937363331333737303439313431000000132d383537323433363035313336393535353030000000142d38363633323838393031353339383831383334000000142d38373235313334303137363133373038383230000000142d38373535353439363530373038383933353238000000142d38383030333037303232343639303033343238000000142d38383839313732333432303033373331383732000000142d38393234363932353333303233383137393232000000142d39303134353238373237353839313232373532000000142d39303138353934343637303935323334393836000000132d393032363537303332383532383237333037000000132d393537353638333035323837393833393939000000133131333932313230313835383330373236393800000013313135343733393632343635343935363233330000001331333537333636373132363537393134333538000000133134313737353635333837373831303937393500000013313437333934363235363738313534393138380000001331363431383136303331313130393032313934000000133137343030373431303633333138383835343100000013313734303333363735333338333638343933380000001331383436303536363939353833363033323538000000133138393839383933353639353236303435323100000013323138383233303235313436333534303237360000001332323330363932323035353030303634303232000000133232383930363838393237373733343531353900000012323333343435373338303735303330333430000000133233333438323030373135363639333338363200000013323334323631383034383039313830363331390000001332333533303434363730353731393035383037000000133234343532333838373135333334303033353800000013323437363239313734383632363538373139340000001332363035373930343439353930363030333435000000133236383831333531383336343932323931343600000013323639333933333334353535323531363434330000001332373638363834353535363330353433343133000000133238383635353739363633363733313537373500000013323931353133333133323230353532353939330000001333303932393637373438373739343138323532000000133331343339303737333936373035313331393100000013333139313139333535333834353738303737330000001333343834373430393737393933373038323232000000123335333837313530383435333034353136380000001333363037353234383934373839393434333836000000133336333535393836383034383931393730343400000013333639363237313331353832363235353033340000001333373136313436303338353431373335323533000000133339353439353635383134383632393934313300000013343038353533333032363332303231313730350000001334303839393935313530323332333438343637000000133431333439373936323138313130343433383800000013343137303536323733323234353931393930320000001234313937313336333538353137303531333100000013343231363031343534313439313133343035360000001334323339303732383033313135373934333737000000133432363833383535313730383332323032333500000013343530323235383632383730383737363531330000001334353237343335303931373238333430393233000000133437343435363335343735313937313433373300000013343832333038323137353635363530343535370000001334383639383232323435393336333235393534000000133438373630373934313235303433313235323500000013343932393134353232333931393337303630360000001235313535303034393933313838353832383300000013353236323532353931363132313230303337330000001335333137313135363430383835333532363032000000133534383437303431373836343539383931323400000013353537313437333334343231303237303934380000001335363538393638393639353335323433353436000000123536373138343639363037393635383332380000001335363939333236353633393233333131363238000000133538363133313532313430383032303938303100000013363030383039373237363338353138353230350000001336303135313737363237393238333530393639000000133630343732343530363132313737363133363200000013363139353238323332333339303030393132390000001336323337393039313138353730303935363235000000133632373431333831343831393839333134393500000013363335323036323132343333363836373535300000001336333933373431383337393039313036343634000000133633393431343036393938313230323334323000000013363430343331313037323632373339323934300000001336343836363538343136393130343039363539000000133635323531343636383736333435303739393200000013363532393339353839303139333032383439320000001336353734323038303738333830393239333537000000133636323230303331313638343132363831353400000013363635343036333232363137383136373236300000001336373332303230383338363336343430313430000000133637393831383035383231333037363236363800000012363830313232363834313534303136373330000000133638343733323139393737323034363237343400000013363836303133373338303430333735343632330000001336393139373936393235333032323238303639000000133730323036333235383336353135343738363000000013373133383339343830323630313738373033360000001337323331363035363033393935363731383831000000133732333436393835303538373034383530323800000013373235323236363733303330333331343334380000001337333737363335323233343130373131363835000000123734303335323033373038353839323039390000001337343236313832333132373734373135303630000000123734333632373030393636323036373639370000001337343930393230313037363639363138333539000000133735303630333238313633353239313837353700000013373531333034343839383730303636393234300000001337353437393232303936313934373330313837000000133735363333333634333535383938393539303300000013373539393035333432353737343038363330360000001337363132383737343835383237303135303935000000133737313034363834353331383134323134393000000013373731373231343932353937373434373837330000001337383037313337303736313137383030323035000000133738333631323333333937363939313937383000000013373838343830343739363738303033343739360000001338303336393934393039313233323136363739000000133830393635303031393738303833323833373800000013383136363436353637363033333538383239380000001338313935313031343332303939333933343931000000133831393836393936373338343633363835373500000013383230383631313230393838313133363134380000001338333737373733313033303635343734393739000000133836383733343833363638313439383138313900000013383734313339373637333830303933353331330000001338373637373332393735343032343835373238000000133838303134343432303630303838303832363200000013383836383933313935353538333730323336320000001338383934303937343833393430323338373032000000133839303038303531363635353432363835303700000013383934343335333533343439383735333338380000001339303032373433333338373630363931373033000000133930383630383538373431353035323433343600000013393232303838303831373933343335313032300000001239343432353432333037363130313839323100000012393438343832363633383530323339323938ffffffff
    84 # version
    00 # flags
    0004 # stream
    08 # opcode
    000019
    8b000000020000000100000012000673797374656d00056c6f63616c00036b6579000d000c626f6f747374726170706564000d001162726f6164636173745f616464726573730010000c636c75737465725f6e616d65000d000b63716c5f76657273696f6e000d000b646174615f63656e746572000d0011676f737369705f67656e65726174696f6e00090007686f73745f6964000c000e6c697374656e5f61646472657373001000176e61746976655f70726f746f636f6c5f76657273696f6e000d000b706172746974696f6e6572000d00047261636b000d000f72656c656173655f76657273696f6e000d000b7270635f616464726573730010000e736368656d615f76657273696f6e000c000e7468726966745f76657273696f6e000d0006746f6b656e730022000d000c7472756e63617465645f61740021000c000300000001000000056c6f63616c00000009434f4d504c4554454400000004ac1100020000000c5465737420436c757374657200000005332e342e300000000b6461746163656e7465723100000004658b6c1d00000010f1f9ecf2ecae4f63968a61fba4b9574a00000004ac11000200000001340000002b6f72672e6170616368652e63617373616e6472612e6468742e4d75726d757233506172746974696f6e6572000000057261636b3100000006332e302e3239000000040000000000000010286d83bc098a392fbccf243455b0e0fe0000000632302e312e300000176900000100000000132d313132353834373632373936383737353637000000142d31313936343036373737333136303230333836000000132d313233373531353738323830333233343235000000142d31323734363039363933313839393132363834000000142d31333036393633373930353834373834353734000000142d31333830373535353134313231343738323835000000142d31343336323333393132393631323436363734000000142d31343433333238353230353339333433353932000000142d31353234363334393234353631323335313031000000142d31353237383632333039383837353431303735000000142d31353238373134323235373533383038323330000000142d31353830323532393435353833343637373130000000122d3136383932333239303035323939343335000000142d31383333323137353732353138353233303433000000142d31393131353133353833373730393232343131000000142d31393338343835303731323635343339353233000000142d32303832373637363230343634303430393534000000142d32313333363133373331373233343036333539000000142d32313336353538353133383031313933313531000000142d32313538313934313132353034383030353836000000142d32313631383637333439363130393832303733000000142d32333537323438323137383237383537323933000000142d32333738383538313535303231333334373636000000142d32343331333133343635393832323837343331000000132d323435333335393736313531383536383632000000132d323438343731383633303530353737333934000000132d323438383935393431343835383639323330000000142d32353238353239363938303633353633373434000000142d32363134363237333531343236333234303037000000142d32363732363037363336323536373339363134000000142d32363936303930333536393136373033323937000000142d32373239333434383332303031323735353630000000142d32383231393830323932373435393834363530000000142d32383733393635313435383932313039313234000000142d32383737333038353934303537353630333632000000142d32393838323339373034393834383736383939000000142d33313033343237373136323038313930303536000000132d333235323233383037303639313237343136000000142d33323935393239343135383635323435323833000000142d33343434373135343630393432393234383734000000142d33343736323837363532383636363837393639000000142d33353630313932333437343739323839323334000000142d33363735363131363531303336353835353831000000142d33363934303835333236393939343432393839000000142d33373733343931383236343232323837323235000000142d33373735393634343938353138313332393639000000142d33373736363336313734333038303336383331000000142d33383031383630313438343838393431303331000000142d33383335333037383130393038383734393739000000142d33393035303332393234383137333430343035000000142d33393035353239353737373231333037373138000000142d33393731333339353232303031393739383436000000142d34303031373434393132313532363837373632000000142d34313135383639393635303037303437313534000000142d34313736343633313230363834343839393038000000142d34323836393233353430313134323539303737000000142d34343036323830303636373837313137393533000000142d34353039323739373938383330333836323132000000142d34353432373832313932333136303430383936000000142d34353638363637393333393031393531333930000000142d34363035373534323734323930373738303134000000142d34363734353434363730363130393030323933000000132d343639363732373539303036373330333836000000132d343834323737313039313936383836343430000000142d34383532333838393232393030393238333033000000142d35303932383639313337303835383139323032000000142d35333436353034313338343233343331323730000000142d35333635363230383434303038393637343639000000142d35353637343531393432333232383038333230000000142d35363039303333373638353134373733363538000000142d35363939333435393637383132353731353337000000122d3537303531303636393136353038363638000000142d35373236393530343430303236353539343432000000142d35373937313335393039303535373039363436000000142d35383636363538343037323730343735363132000000132d353838303934343932343834343639363135000000142d35383835393731353431373335313236353436000000132d353839313338343333323836383530363433000000142d35393036323036373833373537323632303433000000142d35393236383235363833393039303939303732000000142d35393730393437343237343535373632303937000000142d35393833343338313932303635333830383239000000142d36303637353333383437353239353636343838000000132d363135313633363133383631333837383835000000132d363138313030323533373231313939383032000000142d36323134383334323332373834373233383031000000142d36323634343830383138353731313535313333000000142d36323837353438303331383030383437383832000000132d363336363733333833373330303532343338000000142d36343033393131373431323139363033393937000000142d36343132333433353938323636383539313936000000142d36343731373037363739393231333238303734000000142d36363136313334323939363931363737323332000000142d36363239383136383533303830303238383735000000132d363739393831353730363334353032303937000000142d36383336303131353034323431343138383234000000142d36383733343937363335343139323237333530000000142d36383832393238343238313632383939353930000000142d36393634353034303938383631363433363438000000132d373134373834343137313432393938343731000000142d37313735303331353534373935303336353637000000142d37323932333033313432333337333631323135000000142d37333331373532363737363932393934343034000000142d37333730333533383238313838323234303137000000142d37353536313635383538303936383237323937000000142d37363436393037383832333638333839303933000000142d37363739323732333730333537393730383737000000142d37363833313532343131303036323736363037000000142d37363833373630313035333835333035313638000000142d37373733313634343630393933393634393533000000142d37373739373939333736383730313535343633000000142d37383133303739363430393433333130373539000000142d37393232393435353532393334313834383631000000142d37393531353537333437313335333132373636000000142d37393534363635303539353931363535353832000000132d383031333336343530303930353335333834000000142d38303331343030353133303939373038323230000000142d38303434313135363831303331303933373635000000142d38303539313639383735343734343836363536000000142d38313235313333383434353938363830303932000000142d38313936383333303231343535333939363330000000142d38333632363135383234353937353038303235000000142d38343938303937363331333737303439313431000000132d383537323433363035313336393535353030000000142d38363633323838393031353339383831383334000000142d38373235313334303137363133373038383230000000142d38373535353439363530373038383933353238000000142d38383030333037303232343639303033343238000000142d38383839313732333432303033373331383732000000142d38393234363932353333303233383137393232000000142d39303134353238373237353839313232373532000000142d39303138353934343637303935323334393836000000132d393032363537303332383532383237333037000000132d393537353638333035323837393833393939000000133131333932313230313835383330373236393800000013313135343733393632343635343935363233330000001331333537333636373132363537393134333538000000133134313737353635333837373831303937393500000013313437333934363235363738313534393138380000001331363431383136303331313130393032313934000000133137343030373431303633333138383835343100000013313734303333363735333338333638343933380000001331383436303536363939353833363033323538000000133138393839383933353639353236303435323100000013323138383233303235313436333534303237360000001332323330363932323035353030303634303232000000133232383930363838393237373733343531353900000012323333343435373338303735303330333430000000133233333438323030373135363639333338363200000013323334323631383034383039313830363331390000001332333533303434363730353731393035383037000000133234343532333838373135333334303033353800000013323437363239313734383632363538373139340000001332363035373930343439353930363030333435000000133236383831333531383336343932323931343600000013323639333933333334353535323531363434330000001332373638363834353535363330353433343133000000133238383635353739363633363733313537373500000013323931353133333133323230353532353939330000001333303932393637373438373739343138323532000000133331343339303737333936373035313331393100000013333139313139333535333834353738303737330000001333343834373430393737393933373038323232000000123335333837313530383435333034353136380000001333363037353234383934373839393434333836000000133336333535393836383034383931393730343400000013333639363237313331353832363235353033340000001333373136313436303338353431373335323533000000133339353439353635383134383632393934313300000013343038353533333032363332303231313730350000001334303839393935313530323332333438343637000000133431333439373936323138313130343433383800000013343137303536323733323234353931393930320000001234313937313336333538353137303531333100000013343231363031343534313439313133343035360000001334323339303732383033313135373934333737000000133432363833383535313730383332323032333500000013343530323235383632383730383737363531330000001334353237343335303931373238333430393233000000133437343435363335343735313937313433373300000013343832333038323137353635363530343535370000001334383639383232323435393336333235393534000000133438373630373934313235303433313235323500000013343932393134353232333931393337303630360000001235313535303034393933313838353832383300000013353236323532353931363132313230303337330000001335333137313135363430383835333532363032000000133534383437303431373836343539383931323400000013353537313437333334343231303237303934380000001335363538393638393639353335323433353436000000123536373138343639363037393635383332380000001335363939333236353633393233333131363238000000133538363133313532313430383032303938303100000013363030383039373237363338353138353230350000001336303135313737363237393238333530393639000000133630343732343530363132313737363133363200000013363139353238323332333339303030393132390000001336323337393039313138353730303935363235000000133632373431333831343831393839333134393500000013363335323036323132343333363836373535300000001336333933373431383337393039313036343634000000133633393431343036393938313230323334323000000013363430343331313037323632373339323934300000001336343836363538343136393130343039363539000000133635323531343636383736333435303739393200000013363532393339353839303139333032383439320000001336353734323038303738333830393239333537000000133636323230303331313638343132363831353400000013363635343036333232363137383136373236300000001336373332303230383338363336343430313430000000133637393831383035383231333037363236363800000012363830313232363834313534303136373330000000133638343733323139393737323034363237343400000013363836303133373338303430333735343632330000001336393139373936393235333032323238303639000000133730323036333235383336353135343738363000000013373133383339343830323630313738373033360000001337323331363035363033393935363731383831000000133732333436393835303538373034383530323800000013373235323236363733303330333331343334380000001337333737363335323233343130373131363835000000123734303335323033373038353839323039390000001337343236313832333132373734373135303630000000123734333632373030393636323036373639370000001337343930393230313037363639363138333539000000133735303630333238313633353239313837353700000013373531333034343839383730303636393234300000001337353437393232303936313934373330313837000000133735363333333634333535383938393539303300000013373539393035333432353737343038363330360000001337363132383737343835383237303135303935000000133737313034363834353331383134323134393000000013373731373231343932353937373434373837330000001337383037313337303736313137383030323035000000133738333631323333333937363939313937383000000013373838343830343739363738303033343739360000001338303336393934393039313233323136363739000000133830393635303031393738303833323833373800000013383136363436353637363033333538383239380000001338313935313031343332303939333933343931000000133831393836393936373338343633363835373500000013383230383631313230393838313133363134380000001338333737373733313033303635343734393739000000133836383733343833363638313439383138313900000013383734313339373637333830303933353331330000001338373637373332393735343032343835373238000000133838303134343432303630303838303832363200000013383836383933313935353538333730323336320000001338383934303937343833393430323338373032000000133839303038303531363635353432363835303700000013383934343335333533343439383735333338380000001339303032373433333338373630363931373033000000133930383630383538373431353035323433343600000013393232303838303831373933343335313032300000001239343432353432333037363130313839323100000012393438343832363633383530323339323938ffffffff
"""))
sstable.utils.assert_equal(0x84, got_example.version)
sstable.utils.assert_equal(2, got_example.body.kind)
sstable.utils.assert_equal(1, got_example.body.result.metadata.flags)
sstable.utils.assert_equal(18, got_example.body.result.metadata.columns_count)
sstable.utils.assert_equal("system", got_example.body.result.metadata.global_table_spec.keyspace.string)
sstable.utils.assert_equal("local", got_example.body.result.metadata.global_table_spec.table.string)
sstable.utils.assert_equal("key", got_example.body.result.metadata.column_specs[0].name.string)
sstable.utils.assert_equal(13, got_example.body.result.metadata.column_specs[0].type.id)
# eventually these should be strings, not byte arrays:
sstable.utils.assert_equal(b"COMPLETED", got_example.body.result.rows_content[0].row[1].column_value)
sstable.utils.assert_equal(b"3.4.0", got_example.body.result.rows_content[0].row[4].column_value)
sstable.utils.assert_equal(b"datacenter1", got_example.body.result.rows_content[0].row[5].column_value)
