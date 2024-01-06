import re
import textwrap
import inspect
import construct.expr

import cql_struct
import sstable.sstable_data
import sstable.sstable_statistics

def convert_construct(x, depth=0):
    #print("convert_construct", x, depth)
    prefix = "    " * depth
    prefix2 = "    " * (depth+1)
    name = x.__class__.__name__
    if name == "Struct":
        return "\n".join([
            prefix + "(" + name,
            "\n".join([convert_construct(subcon, depth+1) for subcon in x.subcons]),
            prefix + ")",
        ])
    elif name == "function":
        return prefix + "(" + "Function" + " " + str(x) + ")"
    elif name == "method":
        return prefix + "(" + "Method" + " " + str(x) + ")"
    elif name == "BinExpr":
        return prefix + "(" + "BinExpr" + " " + str(x) + ")"
    elif name == "int":
        return prefix + "(" + "Int" + " " + str(x) + ")"
    elif name == "Path":
        p = str(x).replace(r"^this", "")
        field_names = re.findall(r"\['([^']+)'\]", p)
        field_names = ".".join(field_names)
        return prefix + "(" + name + " " + repr(field_names) + ")"
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
        return "\n".join([
            prefix + "(" + name,
            convert_construct(x.length, depth+1),
            prefix + ")",
        ])
    elif name == "Array":
        ## field_path is for example "this['_root']['_']['sstable_statistics']['serialization_header']['clustering_key_count']"
        ## field_path is for example "['sstable_statistics']['serialization_header']['clustering_key_count']"
        #field_path = str(x.count)
        #if not field_path.startswith("this"):
        #    print(f"ERROR: Array count must start with 'this': {field_path}")
        #p = field_path.replace(r"^this", "")
        #field_names = re.findall(r"\['([^']+)'\]", p)
        #field_names = ".".join(field_names)
        #print("1 Path", p, field_names)

        return "\n".join([
            prefix + "(" + name,
            #prefix2 + f"(Path {repr(field_name)})",
            #prefix2 + f"(Path {repr(field_names)})",
            convert_construct(x.count,depth+1),
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
            convert_construct(x.predicate,depth+1),
            convert_construct(x.subcon,depth+1),
            prefix + ")",
        ])
    elif name == "Switch":
        cases = []
        for k, v in x.cases.items():
            cases.append(prefix2 + f"(Case {k}\n{convert_construct(v, depth+2)})")
        return "\n".join([
            prefix + "(" + name,
            convert_construct(x.keyfunc, depth+1),
            "\n".join(cases),
            prefix + ")",
        ])
    elif name == "DynamicSwitch":
        return "\n".join([
            prefix + "(" + name,
            convert_construct(x.key_predicate,depth+1),
            convert_construct(x.value_func,depth+1),
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
            convert_construct(x.condfunc,depth+1),
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
print(convert_construct(sstable.sstable_statistics.statistics_format))
print(convert_construct(cql_struct.frame))
