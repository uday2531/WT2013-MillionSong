# python MapReduce_main.py test.txt
import sys
import MapReduce_util as MR_u
import MapReduce_func as MR_f
from multiprocessing import Pool
if (len(sys.argv) != 2):
    print "Program requires path to file for reading!"
    sys.exit(1)

num_proc = 1;
# Load file, stuff it into a string
text = MR_u.load(sys.argv[1])

# Build a pool of 8 processes
pool = Pool(processes=num_proc,)

# Fragment the string data into 8 chunks
partitioned_text = list(MR_u.chunks(text, len(text) / num_proc))
#print len(text) / num_proc,len(partitioned_text[0])
# Generate count tuples for title-cased tokens

single_count_tuples = pool.map(MR_f.Map_WC, partitioned_text)
#single_count_tuples = pool.map(MR_f.Map_WC, partitioned_text)
"""
map_func = MR_f.Mapper('WordCount')
single_count_tuples = map_func.Map(partitioned_text[0])
token_to_tuples = MR_f.Partition([single_count_tuples])
"""
#print len(single_count_tuples)#
# Organize the count tuples; lists of tuples by token key
token_to_tuples = MR_f.Partition(single_count_tuples)

# Collapse the lists of tuples into total term frequencies
term_frequencies = pool.map(MR_f.Reduce, token_to_tuples.items())

# Sort the term frequencies in nonincreasing order
term_frequencies.sort (MR_u.tuple_sort)

for pair in term_frequencies[:20]:
    print pair[0], ":", pair[1]
