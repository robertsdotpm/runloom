"""n04 -- MQTT 3.1.1 CONNECT over TCP through the real netpoll path.

Sends a CONNECT (protocol 'MQTT' level 4, clean session, random ClientID) to
score-ordered public MQTT brokers and frames the CONNACK.  A byte0/remaining-
length that disagrees with the bytes TCPConn framed is a transport finding; a
CONNACK return code 1-5 (bad version / id rejected / unavailable / bad auth /
not authorized) is an ENV refusal, and only return code 0 (accepted) is a PASS.
Exercises small var-length-prefixed framing over the netpoll TCP path.  Opt-in,
never in any gate.
"""
import os
import socket
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import netrun      # noqa: E402
import netlist     # noqa: E402
import protocols   # noqa: E402
from netrun import EnvSkip, Finding   # noqa: E402

FAMILY = socket.AF_INET


def mqtt_probe(io, srv, timeout_ms):
    client_id = "pygo-net-%s" % os.urandom(4).hex()
    s = io.tcp_connect(srv["ip"], srv["port"], FAMILY, timeout_ms)
    try:
        io.send_all(s, protocols.mqtt_connect_packet(client_id), timeout_ms)
        # CONNACK is a fixed 4-byte frame (type+remaining-length(1)=2, 2-byte body).
        frame = io.recv_n(s, 4, timeout_ms)
        try:
            rc = protocols.mqtt_parse_connack(frame)
        except protocols.MqttParseError as e:
            msg, is_bug = e.args[0], e.args[1]
            if is_bug:
                raise Finding("CONNACK framing corrupt: " + msg)
            raise EnvSkip("MQTT: " + msg)
        if rc == 0:
            return ("pass", "CONNACK accepted")
        raise EnvSkip("CONNACK return code %d (server refusal)" % rc)
    finally:
        try:
            s.close()
        except OSError:
            pass


if __name__ == "__main__":
    sys.exit(netrun.main("n04_mqtt_tcp",
                         [("mqtt-tcp-v4", netlist.CAT_MQTT, "IPv4", "TCP",
                           mqtt_probe)]))
