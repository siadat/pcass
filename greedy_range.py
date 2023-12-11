import construct

# The original construct.GreedyRange._parse method catches *all* exceptionse as
# no matches and it doesn't show any error messages, even when there's no
# match. So, if the grammar is incorrect, it will not produce any useful return
# value or partial match or exceptions or error messages. Nothing.
# I Think it is here: https://sourcegraph.com/python/construct@v2.10.69/-/blob/construct/core.py?L2578-2579
#
# This behaviour is not useful when I am writing and debugging the schema/grammar.
# Therefore I need to implement my own GreedyRange:
class GreedyRangeWithExceptionHandling(construct.GreedyRange):
    def _parse(self, stream, context, path):
        items = []
        while True:
            try:
                item = self.subcon._parse(stream, context, path)
                items.append(item)
            except construct.StreamError:
                break  # EOF reached
            except Exception as e:
                print(f"Exception occurred at position {stream.tell()}: {e}")
                # Decide how to handle the exception (skip, retry, etc.)
                # Example: skip to the next byte
                stream.seek(stream.tell() + 1)
                continue
        return items

