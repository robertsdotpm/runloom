import runloom
results = []
def make(tag):
    def handler():
        results.append(tag)
    return handler
def main():
    for i in range(100):
        runloom.fiber(make(i))
    runloom.sleep(1.0)
runloom.run(2, main)
print(len(set(results)), "distinct tags (expected 100), ran", len(results))
