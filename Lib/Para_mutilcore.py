# python MapReduce_main.py test.txt
import os
from multiprocessing import Pool
"""
a=open('count.txt')
b=a.readline().split()
a.close()
c=open('m1_order.txt','w')
[c.write("%s\n" % item) for item in b[::2]]
c.close()
"""
# Write_Song([0,1,10,'../data/kaggle_visible_evaluation_triplets.txt','m1_order.txt'])
def Get_UserLine(L):
    fp = open(L[2])
    user = ''
    users = []
    for i, lines in enumerate(fp):
        if (i > L[0] and i <= L[1]) or i==0:
            line = lines.split()
            if line[0]!=user:
                if user=='':
                    users.append(line[0])
                users.append(i)
                user = line[0]
        elif i>L[1]:
            break
    fp.close()
    users.append(user)
    return users

def Write_Song(L):
    a=open('mc_'+str(L[0])+'.txt','w')
    result = os.popen("awk 'NR >= "+str(L[1])+" && NR <= "+str(L[2])+"' "+L[3])
    user = ''
    order_f = open(L[4])
    order = order_f.readline()
    print result.readuserlines()
    print len(order)
    for line in result:
        if line[0]==user:
            del user_song[line[1]]
        else:
            user_song = order
            if user!='':
                a.write(user_song)
            user = line[0]
    a.close()


num_proc = 1;
#Write_Song([0,1,10,'../data/kaggle_visible_evaluation_triplets.txt','m1_order.txt'])
# Build a pool of 8 processes
pool = Pool(processes=num_proc,)

test_file='../data/kaggle_visible_evaluation_triplets.txt'
out_file='m1_out.txt'
# 1. get user chunck
tmp_re = os.popen('wc -l '+test_file)
len_test = int(tmp_re.readline().split()[0])
avg_test = len_test/num_proc+1
text_p = [[i*avg_test,(i+1)*avg_test,test_file] for i in range(num_proc)]
text_p[-1][1]=len_test
text_p[0][1] = 100

tmp_re = pool.map(Get_UserLine, text_p)
#tmp_re = Get_UserLine(text_p[0])
print tmp_re
userlines= tmp_re[0][1:-1]
"""
for i in range(1,len(tmp_re)):
    if tmp_re[i][0]==tmp_re[i-1][-1]:        
        userlines.append(tmp_re[i][2:-1])
    else:
        userlines.append(tmp_re[i][1:-1])

#print len(text) / num_proc,len(partitioned_text[0])
# Generate count tuples for title-cased tokens
L = [for i in range(num_proc)]
single_count_tuples = pool.map(Write_Song, L)
#single_count_tuples = pool.map(MR_f.Map_WC, partitioned_text)
#print len(single_count_tuples)#
# Organize the count tuples; lists of tuples by token key
token_to_tuples = MR_f.Partition(single_count_tuples)

# Collapse the lists of tuples into total term frequencies
term_frequencies = pool.map(MR_f.Reduce, token_to_tuples.items())

# Sort the term frequencies in nonincreasing order
term_frequencies.sort (MR_u.tuple_sort)

out = open('count.txt','w')
for pair in term_frequencies:
    out.write(pair[0]+' '+str(pair[1])+' ')
out.close()
"""
