import runloom
def main():
    print(runloom.gather(lambda: 1, lambda: 2))
runloom.run(1, main)
