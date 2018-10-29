from nltk.tokenize import word_tokenize
import numpy as np

def data_stream():
    """Stream the data in 'leipzig100k.txt' """
    with open('leipzig100k.txt', 'r') as f:
        for line in f:
            for w in word_tokenize(line):
                if w.isalnum():
                    yield w

def bloom_filter_set():
    """Stream the data in 'Proper.txt' """
    with open('Proper.txt', 'r') as f:
        for line in f:
            yield line.strip()



############### DO NOT MODIFY ABOVE THIS LINE #################


# Implement a universal hash family of functions below: each function from the
# family should be able to hash a word from the data stream to a number in the
# appropriate range needed.


# import nltk
# nltk.download('punkt')


#compare actual second moment vs estimated set moment.


def uhf(p,rng):
    """Returns a hash function that can map a word to a number in the range
    0 - rng
    """
    a = np.random.randint(1, p)
    b = np.random.randint(0, p)

    return lambda x: ((a*x+b)%p)%rng


#testing uhf
# hf = uhf(1000003, 101)
# print(type(hf))
# print(hf(1000))
# print(hf)


###############

################### Part 1 ######################
print("STARTING PART 1")
from bitarray import bitarray

import time
import math


size = 2**18   # size of the filter
p = 1000003

hash_fns = [uhf(p,size), uhf(p,size), uhf(p,size), uhf(p,size), uhf(p,size)]  # place holder for hash functions
bloom_filter = size*bitarray('0')
num_words = 0         # number in data stream
num_words_in_set = 0  # number in Bloom filter's set

# print(hash_fns)

t0 = time.time()


# def convertToNumber(s):
#     return int.from_bytes(s.encode(), 'little')

from collections import defaultdict
print('creating bloom filter, takes < 200 seconds on my machine')
for word in bloom_filter_set(): # add the word to the filter by hashing etc.

   l = len(word)
   word_key = sum([ord(word[c])*(c+1)*l for c in range(l)]) #unique ascii sum based on order of chars in word.

   # word_key = convertToNumber(word)

   for f in range(len(hash_fns)):
        pos = hash_fns[f](word_key) # get pos to hash to
        # print(pos)
        del bloom_filter[pos]
        bloom_filter.insert(pos, 1)

t1 = time.time()

total = t1-t0
print('time to hash into bloom filter', total, 'seconds')
# print(bloom_filter)

t2 = time.time()
for word in data_stream():  # check for membership in the Bloom filter
    num_words += 1
    canda_word = 0
    word_key = sum([ord(word[c]) * c + 1 for c in range(len(word))])
    if num_words % 100000 == 0:
        print('at word',num_words,'in data stream')
    for f in range(len(hash_fns)):
        pos = hash_fns[f](word_key)  # get pos to hash to
        # print(pos)
        # del bloom_filter[pos]
        # bloom_filter.insert(pos, 1)
        if bloom_filter[pos] == 1:
            canda_word += 1
    if canda_word == len(word):
        num_words_in_set += 1

t3 = time.time()
total = t3-t2

print('time to check datastream',total)

print('Total number of words in stream = %s'%(num_words,))
print('Total number of words in set = %s'%(num_words_in_set,))
FPR = ((num_words_in_set-32657)/num_words_in_set)
print('False positive rate:', FPR)

# ################### Part 2 ######################
print("STARTING PART 2")
hash_range = 24 # number of bits in the range of the hash functions
p = 1000003
size = 2**24
# bit_vector = size*bitarray('0')
fm_hash_functions = []  # Create the appropriate hashes here
for i in range(35):
    fm_hash_functions.append(uhf(p,size))
# print(fm_hash_functions)
def num_trailing_bits(n):
    """Returns the number of trailing zeros in bin(n)

    n: integer
    """
    binary = "{0:b}".format(n)
    return len(binary) - len(binary.rstrip('0'))

num_distinct = 0
wordcount = 2059856
g1 = g2 = g3 = g4 = g5 = 0
counter = 0

t4 = time.time()
for word in data_stream(): # Implement the Flajolet-Martin algorithm
    counter += 1
    ests = []
    l = len(word)
    word_key = sum([ord(word[c]) * (c + 1) * l for c in range(l)])  # unique ascii sum based on order of chars in word.
    if counter % 100000 == 0:
        print('at word',counter,'in data stream')
    for f in range(len(fm_hash_functions)):
        pos = fm_hash_functions[f](word_key)
        est = 2**num_trailing_bits(pos)
        # print(num_distinct)
        ests.append(est)
    # print(ests)
    g1 += sum(ests[:7])
    g2 += sum(ests[:14])
    g3 += sum(ests[:21])
    g4 += sum(ests[:28])
    g5 += sum(ests[:35])

t5 = time.time()
total = t5 - t4
print("part2 time:", total)
group_estimates = [g1/wordcount,g2/wordcount,g3/wordcount,g4/wordcount,g5/wordcount]
import statistics as st
median = st.median(group_estimates)
print("Averages:", group_estimates)
print("Estimate of number of distinct elements = %s"%(round(median),))

################### Part 3 ######################
print("STARTING PART 3")
var_reservoir = [0]*512
second_moment = 0
third_moment = 0
wordcount = 2059856

# You can use numpy.random's API for maintaining the reservoir of variables

counter = 0


for word in data_stream(): # Imoplement the AMS algorithm here
   counter += 1
   if counter % 100000 == 0:
       print('at word',counter,'in data stream')

   res_samp = np.random.randint(0, 511)
   l = len(word)
   word_key = sum([ord(word[c]) * (c + 1) * l for c in range(l)])
   # var_reservoir[pos] = uhf(p,size)(word_key)
   pos = uhf(p,size)(word_key)
   z = 0
   if pos > z:
       z = pos
   var_reservoir[res_samp] = 2**(num_trailing_bits(z))

# print(var_reservoir)
second_moment = np.var(var_reservoir)
test_second_moment =  sum(np.power(var_reservoir, 2))/len(var_reservoir)
print('testing another method for 2nd moment calc:',test_second_moment)
third_moment = sum(np.power(var_reservoir, 3))/len(var_reservoir)


print("Estimate of second moment = %s"%(second_moment,))
print("Estimate of third moment = %s"%(third_moment,))
