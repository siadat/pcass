import construct

import sstable.type_parser

class DynamicSwitch(construct.Construct):
    def __init__(self, type_name_predicate):
        super().__init__()
        self.type_name_predicate = type_name_predicate

    def _parse(self, stream, context, path):
        type_name = self.type_name_predicate(context)
        struct = sstable.type_parser.parser.parse(type_name)
        print("DynamicSwitch _parse type_name", type_name, struct)
        return struct._parse(stream, context, path)

    def _build(self, obj, stream, context, path):
        print("DynamicSwitch _build obj ", obj)
        # stream.write(build(obj))
        return obj
