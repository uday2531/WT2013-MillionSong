__author__ = 'james'

import pandas as pd

def import_dataset(filename):
    result = pd.read_csv(filename, sep='\t', names=['userID','songID','popularity'], header=False)
    return result

def _convert_user_id_to_user_ind(user_ids):
    filename = '../data/kaggle_users.txt'
    user_inds = pd.read_csv(filename, header=False, names=['userID', ])

if __name__ == '__main__':
    dataset = import_dataset("../data/kaggle_visible_evaluation_triplets.txt")
    print dataset.head()
