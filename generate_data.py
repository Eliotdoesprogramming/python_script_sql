# we find that python doesnt allow us to append from multiple threads to the same file, shit completely breeaks
import threading
import time
start = time.time()
with open('data.csv', 'w') as f:
    f.write('id,name\n')
def write_rows(filename, index, num_rows):
    for i in range(index, index + num_rows):
        with open(filename, 'a+') as f:
            f.write(str(i)+f",'name{i}'\n")
        if(i%1000 == 0 ):
            print('wrote row '+ str(i))
threads = []
for i in range(100):
    t = threading.Thread(target=write_rows, args=('threaded_mill.csv', i*10000, 10000))
    t.start()
    threads.append(t)
for t in threads:
    t.join()
print('Done')
print('finished in ',time.time() - start, 'seconds')