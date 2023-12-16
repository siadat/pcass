import construct

import sstable.type_parser

class DynamicSwitch(construct.Construct):
    def __init__(self, type_name_predicate):
        super().__init__()
        self.type_name_predicate = type_name_predicate

    def _parse(self, stream, context, path):
        type_name = self.type_name_predicate(context)
        struct = sstable.type_parser.parser.parse(type_name)
        return struct._parse(stream, context, path)

    def _build(self, obj, stream, context, path):
        type_name = self.type_name_predicate(context)
        struct = sstable.type_parser.parser.parse(type_name)
        stream.write(struct.build(obj))
        return obj
