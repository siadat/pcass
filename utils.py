import textwrap

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


def assert_equal(want, got):
    try:
        assert want == got
    except AssertionError as e:
        message = textwrap.dedent(f"""
        Error: {e}
        Want:  {want}
        Got:   {got}
        """)
        raise AssertionError(message) from None

