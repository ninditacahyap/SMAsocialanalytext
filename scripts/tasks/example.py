from redis import Redis
import time
import rq

def count(x, y):
    print('Start!')
    for i in range(x,y):
        print(i)
        time.sleep(1)
    print('Finished')
    return 2, 6