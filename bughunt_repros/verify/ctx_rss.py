import runloom, runloom.context as ctx, os
def rss():
    with open('/proc/self/status') as f:
        for line in f:
            if line.startswith('VmRSS'):
                return int(line.split()[1])
def main():
    parent, _ = ctx.WithCancel(ctx.Background())
    base = rss()
    for i in range(200000):
        child, cancel = ctx.WithCancel(parent)
        cancel()
    print("children:", len(parent._children), "RSS growth KB:", rss() - base)
runloom.run(1, main)
