Rules of exchange:

  Unframed OPTIONS-and-SUPPORTED exchanges, when $negotiated_protocol_version is not set yet
    This is the first message exchanged
    This can happen any number of times
    1. Client sends an unframed OPTIONS
    2. Server sends an unframed SUPPORTED

  Framed OPTIONS-and-SUPPORTED exchanges, when $negotiated_protocol_version==v5
    This can happen any number of times
    1. Client sends a framed OPTIONS
    2. Server sends a framed SUPPORTED

  Set $negotiated_protocol_version
    This is also known as "the connection"
    1. Client sends an unframed STARTUP
    2. Server sends an unframed READY or AUTHENTICATE
    Always unframed
    Now we have a $negotiated_protocol_version and "the connection" is established
