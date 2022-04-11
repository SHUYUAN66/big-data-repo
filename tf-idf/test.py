#!/usr/bin/env python
"""reducer.py"""

# python mapper.py < input.txt | sort | python reducer.py
# hadoop jar /usr/lib/hadoop/hadoop-streaming.jar -files mapper.py,reducer.py -mapper mapper.py -reducer reducer.py -input /user/inputs/inaugs.tar.gz -output /user/j_singh/inaugs

import collections
from operator import itemgetter
import sys
import numpy as np
import pprint
import math
from numpy import linalg as LA
pp = pprint.PrettyPrinter(indent = 4, width = 80)

def main(argv):
    current_word = None
    current_fnam = None
    current_count = 0
    word = None
    wcss = {}

    # input comes from STDIN
    for line in sys.stdin:
        # remove leading and trailing whitespace
        line_ = line.strip()

        # parse the input we got from mapper.py
        _, word, fnam, count = line_.split('\t', 3)

        # convert count (currently a string) to int
        try:
            count = int(count)
            if fnam in wcss:
                wcss[fnam].update({word:count})
            else:
                wcss[fnam] = collections.Counter()
                wcss[fnam].update({word:count})

        except ValueError:
            # count was not a number, so silently
            # ignore/discard this line
            pass
    tfidf = calculateTFIDF(wcss) # Implement this function
   

def calculateTFIDF(wcss):
    expected = '''
    
    fnam1
        word1 tfidf1
        word2 tfidf2
        word3 tfidf3
          ::
        wordn tfidfn
    '''
    bows = []
    wordset = set()
    for i in sorted(wcss):
        bows+=list(wcss[i].keys())  
        wordset = wordset.union(set(wcss[i].keys()))

    N=len(wcss.keys())
    counter = dict(collections.Counter(bows))
    idf_diz=dict.fromkeys(wordset,0)
    for w in wordset:
      idf_diz[w] = np.log((1+N)/(1+counter[w]))+1
    
    for fname in sorted(wcss):
        bow = wcss[fname]
        print(fname)
        #calculateTF
        termfreq_diz = dict.fromkeys(wordset,0)
        counter1 =  dict(collections.Counter(bow))
        for w in bow:
            termfreq_diz[w]=counter1[w]/len(bow)
        tf_idf_diz = dict.fromkeys(wordset,0)
        for w in wordset:
            tf_idf_diz[w]=termfreq_diz[w] * idf_diz[w]
        tf_idf_diz[w]=termfreq_diz[w]*idf_diz[w]
        tdidf_values = list(tf_idf_diz.values())
        l2_norm = LA.norm(tdidf_values) 
        tf_idf_norm = {w:tf_idf_diz[w]/l2_norm for w in wordset}
        sorted_tf_idf_norm = dict( sorted(tf_idf_norm.items(), key = lambda item: item[1], reverse = True))
        for w in sorted_tf_idf_norm:
            print("\t" + w + " " + str(sorted_tf_idf_norm[w]))
    return None
if __name__ == "__main__":
    main(sys.argv)
