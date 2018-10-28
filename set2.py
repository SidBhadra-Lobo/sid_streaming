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

def uhf(p,rng):
    """Returns a hash function that can map a word to a number in the range
    0 - rng
    """
    a = np.random.randint(1, p)
    b = np.random.randint(0, p)

    return lambda x: ((a*x+b)%p)%rng

############### 

################### Part 1 ######################

from bitarray import bitarray
size = 2**18   # size of the filter

hash_fns = [None, None, None, None, None]  # place holder for hash functions
bloom_filter = None
num_words = 0         # number in data stream
num_words_in_set = 0  # number in Bloom filter's set

#for word in bloom_filter_set(): # add the word to the filter by hashing etc.
#    pass 

#for word in data_stream():  # check for membership in the Bloom filter
#    pass 

print('Total number of words in stream = %s'%(num_words,))
print('Total number of words in stream = %s'%(num_words_in_set,))
      
################### Part 2 ######################

hash_range = 24 # number of bits in the range of the hash functions
fm_hash_functions = [None]*35  # Create the appropriate hashes here

def num_trailing_bits(n):
    """Returns the number of trailing zeros in bin(n)

    n: integer
    """
    pass

num_distinct = 0

#for word in data_stream(): # Implement the Flajolet-Martin algorithm
#    pass

print("Estimate of number of distinct elements = %s"%(num_distinct,))

################### Part 3 ######################

var_reservoir = [0]*512
second_moment = 0
third_moment = 0

# You can use numpy.random's API for maintaining the reservoir of variables

#for word in data_stream(): # Imoplement the AMS algorithm here
#    pass 
      
print("Estimate of second moment = %s"%(second_moment,))
print("Estimate of third moment = %s"%(third_moment,))
