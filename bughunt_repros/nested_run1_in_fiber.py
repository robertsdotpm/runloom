"""run(1) called from INSIDE a fiber while run(1) is driving -- claimed supported re-entrancy."""
import faulthandler
faulthandler.dump_traceback_later(15, exit=True)
import runloom

order = []
def main():
    def other():
        order.append("other")
    runloom.fiber(other)
    def inner():
        order.append("inner")
    order.append("before")
    runloom.run(1, inner)   # from inside a running fiber
    order.append("after")

runloom.run(1, main)
print(order)
