# Wait for netstat to show that port 9042 is listening
while ! netstat -tuln | grep -q 9042; do sleep 1; done

tshark -l -i lo -Y "tcp.port == 9042" -T fields -d tcp.port==9042,echo \
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
