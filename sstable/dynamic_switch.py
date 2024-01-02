import construct

class DynamicSwitch(construct.Construct):
    def __init__(self, key_predicate, value_func):
        super().__init__()
        self.key_predicate = key_predicate
        self.value_func = value_func

    def _parse(self, stream, context, path):
        key = self.key_predicate(context)
        struct = self.value_func(key)
        return struct._parse(stream, context, path)

    def _build(self, obj, stream, context, path):
        key = self.key_predicate(context)
        struct = self.value_func(key)
        stream.write(struct.build(obj))
        return obj
