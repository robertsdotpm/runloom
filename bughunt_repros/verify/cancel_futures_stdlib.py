import time, concurrent.futures as cf

def main():
    ex = cf.ThreadPoolExecutor(max_workers=1)
    ran = []
    ex.submit(lambda: (time.sleep(0.3), ran.append(1)))
    f2 = ex.submit(lambda: ran.append(2))
    ex.shutdown(wait=True, cancel_futures=True)
    print("f2.cancelled():", f2.cancelled(), "ran:", ran)

main()
