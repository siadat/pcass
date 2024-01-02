import sstable.positioned_construct
import sstable.utils

def debug_rows(struct, byts):
    sstable.positioned_construct.init()
    err = None
    err_pos = None
    err_traceback = None
    import io
    f = io.BytesIO(byts)
    try:
        out = struct.parse_stream(f)
        print(out)
    except Exception as e:
        print("Error: %s" % e)
        err = e
        err_pos = f.tell()
        import traceback
        err_traceback = traceback.format_exc()
    last_pos = f.tell()
    print("Last position: %d" % last_pos)
    import os
    sstable.positioned_construct.pretty_hexdump("ok", io.BytesIO(byts), last_pos, os.sys.stdout, err, err_pos, err_traceback)
