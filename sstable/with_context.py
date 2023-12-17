import box
import construct

import sstable.utils

class WithContext(construct.Subconstruct):
    def __init__(self, subcon, **kw_ctx_funcs):
        super().__init__(subcon)
        self.subcon = subcon
        self.kw_ctx_funcs = kw_ctx_funcs

    def parse_subcon(subcon, stream, parent_path, **contextkw):
        r"""
        This is the same as parse_stream, except path is not reset and parent_path is used instead of "(parsing)".
        See https://sourcegraph.com/github.com/construct/construct@c25a47172d4bde392b7ad188175b07b319d3dea4/-/blob/construct/core.py?L406
        """
        context = construct.Container(**contextkw)
        context._parsing = True
        context._building = False
        context._sizing = False
        context._params = context
        try:
            return subcon._parsereport(stream, context, parent_path)
        except construct.CancelParsing:
            pass

    def _parse(self, stream, context, path):
        kwargs = context.copy()
        for name, func in self.kw_ctx_funcs.items():
            kwargs[name] = func(context)
        return WithContext.parse_subcon(self.subcon, stream, path, **kwargs)

    def _build(self, obj, stream, context, path):
        for name, func in self.kw_ctx_funcs.items():
            context[name] = func(context)
        self.subcon._build(obj, stream, context, path)

child = construct.Struct(
    "bytes" / construct.Bytes(lambda ctx: ctx._.row.length_from_some_parent),
    "value_from_root" / construct.Computed(lambda ctx: ctx._._root.some_root_value),
)

parent = construct.Struct(
    "length_from_some_parent" / construct.Int8ub,
    "some_root_value" / construct.Computed(lambda ctx: "abc"),
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
    "some_root_value": "abc",
    "child": {
        "grand_grand_child": {
            "grand_child": {
                "child": {
                    "bytes": b"\x01\x02\x03",
                    "value_from_root": "abc",
                },
            },
        },
    },
}))