# I want to be able to use tshark without sudo:
#   sudo usermod -aG wireshark sina
#   sudo chmod +x /usr/bin/dumpcap
#   sudo chmod u+s /usr/bin/dumpcap

set -x
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
