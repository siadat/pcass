import textwrap
import inspect
import construct.expr

import sstable.sstable_data
import sstable.sstable_statistics

def convert_construct(x, depth=0):
    prefix = "    " * depth
    prefix2 = "    " * (depth+1)
    name = x.__class__.__name__
    if name == "Struct":
        return "\n".join([
            prefix + "(" + name,
            "\n".join([convert_construct(subcon, depth+1) for subcon in x.subcons]),
            prefix + ")",
        ])
    elif name == "Renamed":
        name = "Field"
        return "\n".join([
            prefix + "(" + name + " " + repr(str(x.name)),
            convert_construct(x.subcon,depth+1),
            prefix + ")",
        ])
    elif name == "FormatField":
        return prefix + "(" + name + " " + repr(str(x.fmtstr)) + ")"
    elif name == "Bytes":
        return prefix + "(" + name + " " + str(x.length) + ")"
    elif name == "Array":
        # field_path is for example "this['_root']['_']['sstable_statistics']['serialization_header']['clustering_key_count']"
        import re
        # field_path is for example "['sstable_statistics']['serialization_header']['clustering_key_count']"
        field_path = str(x.count)
        p = field_path.replace(r"^this", "")
        field_names = re.findall(r"\['([^']+)'\]", p)
        field_names = ".".join(field_names)
        print("1 Path", p, field_names)

        return "\n".join([
            prefix + "(" + name,
            #prefix2 + f"(Path {repr(field_name)})",
            prefix2 + f"(Path {repr(field_names)})",
            convert_construct(x.subcon,depth+1),
            prefix + ")",
        ])
    elif name == "Pass":
        return prefix + "(" + name + ")"
    elif name == "Tell":
        return prefix + "(" + name + ")"
    elif name == "EnabledColumns":
        return prefix + "(" + name + ")"
    elif name == "VarInt":
        return prefix + "(" + name + ")"
    elif name == "WithContext":
        return "\n".join([
            prefix + "(" + name,
            prefix2 + f"{x.kw_ctx_funcs}",
            convert_construct(x.subcon,depth+1),
            prefix + ")",
        ])
    elif name == "Hex":
        return "\n".join([
            prefix + "(" + name,
            convert_construct(x.subcon,depth+1),
            prefix + ")",
        ])
    elif name == "RepeatUntil":
        return "\n".join([
            prefix + "(" + name,
            prefix2 + f"{x.predicate}",
            convert_construct(x.subcon,depth+1),
            prefix + ")",
        ])
    elif name == "Switch":
        return "\n".join([
            prefix + "(" + name,
            prefix2 + f"{x.keyfunc}",
            prefix + ")",
        ])
    elif name == "DynamicSwitch":
        return "\n".join([
            prefix + "(" + name,
            prefix2 + f"{x.key_predicate}",
            prefix2 + f"{x.value_func}",
            prefix + ")",
        ])
    elif name == "GreedyRangeWithExceptionHandling":
        return "\n".join([
            prefix + "(" + name,
            convert_construct(x.subcon,depth+1),
            prefix + ")",
        ])
    elif name == "IfThenElse":
        return "\n".join([
            prefix + "(" + name,
            prefix2 + f"{x.condfunc}",
            convert_construct(x.thensubcon,depth+1),
            convert_construct(x.elsesubcon,depth+1),
            prefix + ")",
        ])
    elif name == "StringEncoded":
        return "\n".join([
            prefix + "(" + name + " " + repr(str(x.encoding)),
            convert_construct(x.subcon,depth+1),
            prefix + ")",
        ])
    else:
        raise Exception(f"Unknown construct: {name}")

print(convert_construct(sstable.sstable_data.data_format))
print(convert_construct(sstable.sstable_statistics.statistics_format ))
