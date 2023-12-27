sudo tshark -i lo -Y "tcp.port == 9042" -T fields -d tcp.port==9042,echo \
    -e frame.time \
    -e ip.src \
    -e tcp.srcport \
    -e ip.dst \
    -e tcp.dstport \
    -e tcp.len \
    -e tcp.seq \
    -e tcp.stream \
    -e echo.data

    # -e tcp.flags \
    # -e tcp.ack \
