Messages as defined in [native_protocol_v5.md](https://raw.githubusercontent.com/apache/cassandra/cassandra-5.0-beta1/doc/native_protocol_v5.spec):

- Messages
  - Requests
    [ ] STARTUP
    [ ] AUTH_RESPONSE
    [ ] OPTIONS
    [ ] QUERY
    [ ] PREPARE
    [ ] EXECUTE
    [ ] BATCH
    [ ] REGISTER
  - Responses
    [ ] ERROR
    [ ] READY
    [ ] AUTHENTICATE
    [ ] SUPPORTED
    [ ] RESULT
      [ ] Void
      [ ] Rows
      [ ] Set_keyspace
      [ ] Prepared
      [ ] Schema_change
    [ ] EVENT
    [ ] AUTH_CHALLENGE
    [ ] AUTH_SUCCESS
