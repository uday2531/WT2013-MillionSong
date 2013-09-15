"""
If a token has been identified to contain
non-alphanumeric characters, such as punctuation,
assume it is leading or trailing punctuation
and trim them off. Other internal punctuation
is left intact.
"""
def sanitize(w):
  # Strip punctuation from the front
  while len(w) > 0 and not w[0].isalnum():
    w = w[1:] 
  # String punctuation from the back
  while len(w) > 0 and not w[-1].isalnum():
    w = w[:-1] 
  return w
"""
Load the contents the file at the given
path into a big string and return it.
"""
def load(path,sep_str=' '):
  word_list = []
  f = open(path, "r")
  for line in f:
    word_list.append (line)
 
  # Efficiently concatenate Python string objects
  return (''.join(word_list)).split(sep_str)
 
"""
A generator function for chopping up a given list into chunks of
length n.
"""
def chunks(l, n):
  for i in xrange(0, len(l), n):
    yield l[i:i+n]
 
"""
Sort tuples by term frequency, and then alphabetically.
"""
def tuple_sort (a, b):
  if a[1] < b[1]:
    return 1
  elif a[1] > b[1]:
    return -1
  else:
    return cmp(a[0], b[0])
 
