import runloom
res = []
def outer():
    def inner():
        res.append('inner-ran')
    n = runloom.run(1, inner)
    res.append(('returned', 'inner_done' if 'inner-ran' in res else 'INNER NOT RUN YET', 'count', n))
runloom.run(4, outer)
print(res)
