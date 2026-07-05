import runloom
from runloom.sync import Semaphore
def main():
    sem = Semaphore(2)
    sem.acquire(2)
    try:
        sem.release(5)
    except ValueError as e:
        print("raised:", e)
    print("held:", sem._held, "try_acquire(2) ->", sem.try_acquire(2))
runloom.run(1, main)
