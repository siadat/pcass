import tempfile
import textwrap
import subprocess

def bin(number):
    """
    Converts a number to its binary representation, padding each byte with zeros and separating bytes with spaces.
    """
    # Convert to binary and remove the '0b' prefix
    binary_str = format(number, 'b')

    # Calculate padding using bit manipulation and pad the binary string
    padding_length = (-len(binary_str)) % 8
    padded_binary_str = '0' * padding_length + binary_str

    # Split the string into chunks of 8 bits using list comprehension
    return ' '.join(padded_binary_str[i:i+8] for i in range(0, len(padded_binary_str), 8))


def byte_repr(byte):
    if 32 <= byte <= 126:
        s = bytes([byte])
        s = repr(s.decode("utf-8"))
    else:
        s = '───'
    binary = format(byte, '08b')
    return f"0x{byte:02x}\tb{binary}\t{byte:>3}\t{s}"


assertion_count = 0
def assert_equal(want, got):
    global assertion_count
    try:
        assert want == got
        assertion_count += 1
    except AssertionError:
        if isinstance(want, bytes) and isinstance(got, bytes):
            with tempfile.TemporaryDirectory() as dir:
                with open(f"{dir}/want", "w") as f:
                    for byt in want:
                        f.write(byte_repr(byt) + "\n")
                with open(f"{dir}/got", "w") as f:
                    for byt in got:
                        f.write(byte_repr(byt) + "\n")
                subprocess.run(["git", "--no-pager", "diff", "--no-index", "--color", f"{dir}/want", f"{dir}/got"])
        message = textwrap.dedent(f"""
        -   Want:  {want}
        +   Got:   {got}
        """)
        raise AssertionError(message) from None

def print_test_stats():
    global assertion_count
    print(f"{assertion_count} successful assertions completed")
