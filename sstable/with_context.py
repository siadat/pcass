import box
import construct

import sstable.utils

class WithContext(construct.Construct):
    def __init__(self, subcon, **kw_ctx_funcs):
        super().__init__()
        self.subcon = subcon
        self.kw_ctx_funcs = kw_ctx_funcs

    def _parse(self, stream, context, path):
        kwargs = {}
        for name, func in self.kw_ctx_funcs.items():
            kwargs[name] = func(context)
        return self.subcon.parse_stream(stream, **kwargs)

    def _build(self, obj, stream, context, path):
        for name, func in self.kw_ctx_funcs.items():
            context[name] = func(context)
        self.subcon._build(obj, stream, context, path)

child = construct.Struct(
    "bytes" / construct.Bytes(lambda ctx: ctx._.row.length_from_some_parent),
)

parent = construct.Struct(
    "length_from_some_parent" / construct.Int8ub,
    "child" / construct.Struct(
        "grand_grand_child" / construct.Struct(
            "grand_child" / construct.Struct(
                "child" / WithContext(child, row=lambda ctx: ctx._._._),
            ),
        ),
    ),
)

parsed = parent.parse(b"\x03\x01\x02\x03")
sstable.utils.assert_equal(b"\x01\x02\x03", parsed.child.grand_grand_child.grand_child.child.bytes)
sstable.utils.assert_equal(b"\x03\x01\x02\x03", parent.build({
    "length_from_some_parent": 3,
    "child": {
        "grand_grand_child": {
            "grand_child": {
                "child": {
                    "bytes": b"\x01\x02\x03",
                },
            },
        },
    },
}))
