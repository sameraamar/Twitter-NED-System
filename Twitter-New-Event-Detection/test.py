

mylist = {}


import time
import linalg_helper as la

for hash in range(10):
    mylist[hash] = []
    for n in range(1000):
        d = la.Document(str(n), [])
        mylist[hash].append(d)


base = time.time()
for hash in range(10):
    for n in range(100000):
        d = la.Document(str(n), [])
        mylist[hash].append(d)
        mylist[hash].pop(0)

print(len(mylist), time.time() - base)

base = time.time()
for hash in range(10):
    for n in range(100000):
        d = la.Document(str(n), [])
        mylist[hash].append(d)
        mylist[hash] = mylist[hash][1:]

print(len(mylist), time.time() - base)



