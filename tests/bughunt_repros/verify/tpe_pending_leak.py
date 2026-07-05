import runloom.monkey as monkey
monkey.patch()
import concurrent.futures as cf
import runloom_c as rc

def main():
    ex = cf.ThreadPoolExecutor(max_workers=4)
    for i in range(10000):
        ex.submit(lambda: None).result()
    print("len(_pending) after 10000 completed submits:", len(ex._pending))

rc.fiber(main); rc.run()
