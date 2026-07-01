import runloom.monkey as monkey
monkey.patch()
import socket
import runloom_c as rc

out = {}
def f():
    try:
        res = socket.getaddrinfo("mysvc", 80, socket.AF_INET, socket.SOCK_STREAM)
        out["r"] = "RUNLOOM OK: " + res[0][4][0]
    except socket.gaierror as e:
        out["r"] = "RUNLOOM FAIL: %r" % (e,)
    # second call: negative cache means no second query hits the server
    try:
        socket.getaddrinfo("mysvc", 80, socket.AF_INET, socket.SOCK_STREAM)
        out["r2"] = "second call OK"
    except socket.gaierror as e:
        out["r2"] = "second call FAIL: %r" % (e,)

rc.fiber(f)
rc.run()
print(out["r"])
print(out["r2"])
