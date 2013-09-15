import MapReduce_util as MR_u


def Map_SC(L):
    results = []
    # w is a line of the file: user id, song id, song count
    for w in L:
        w2 = w.split('\t')
        results.append((w2[1], int(w2[2])))
                                
    return results

def Map_WC(L):
    results = []
    for w in L:
        # True if w contains non-alphanumeric characters
        if not w.isalnum():
                w = MR_u.sanitize(w) 
        # True if w is a title-cased token
        if w.istitle():
                results.append((w, 1)) 
    return results

"""
Group the sublists of (token, 1) pairs into a term-frequency-list
map, so that the Reduce operation later can work on sorted
term counts. The returned result is a dictionary with the structure
{token : [(token, 1), ...] .. }
"""
def Partition(L):
    tf = {}
    for sublist in L:        
        for p in sublist:
            # Append the tuple to the list in the map            
            try:
                tf[p[0]].append (p)
            except KeyError:
                tf[p[0]] = [p]
    return tf
 
"""
Given a (token, [(token, 1) ...]) tuple, collapse all the
count tuples from the Map operation into a single term frequency
number for this token, and return a final tuple (token, frequency).
"""
def Reduce(Mapping):
    return (Mapping[0], sum(pair[1] for pair in Mapping[1]))

