import runloom as rc
import runloom.monkey as monkey
monkey.patch()
import concurrent.futures as cf

def main():
    ex = cf.ThreadPoolExecutor(max_workers=2)
    f = ex.submit(lambda: 42)
    print("inside run():", f.result(timeout=2))

rc.run(4, main)
