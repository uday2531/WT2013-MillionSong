__author__ = 'james'

import sys
import pandas as pd


class Model:

    _TEST = 1
    top_n_users = 5

    def __init__(self, **kwargs):
        if kwargs.has_key("max_recommendations"):
            self._max_recommendations = kwargs['max_recommendations']

    def _preprocess_train(self, data_train=None):
        self._sumcount_train = data_train[['songID', 'popularity']].groupby('songID').sum()

    def setMaxRecommendations(self, max_songs):
        self._max_recommendations = max_songs
        return self

    def train(self, data_train):
        self._preprocess_train(data_train)
        self._order_train = self._sumcount_train.sort('popularity', ascending=False)

    def predict(self, data_test):
        test_per_user = data_test.groupby('userID')
        recommendation_dict = {}
        count = 0
        for user_name, user_group in test_per_user:

            if self._TEST == 1 and count == self.top_n_users:
                break

            user_recommendation = self._order_train.copy(deep=True)
            sys.stdout.flush()
            print "predicting for user: %s\r" % user_name,
            for index, user_info in user_group.iterrows():
                # exclude songs that users have already heard
                user_recommendation = user_recommendation.drop(user_info['songID'], axis=0)

            recommendation_dict[user_name] = user_recommendation[:self._max_recommendations].index
            count += 1
        # form recommendation dataframe
        if self._TEST == 1:
            recommend_df = pd.DataFrame(recommendation_dict).transpose()
        else:
            recommend_df = pd.DataFrame(recommendation_dict).transpose()
        self._recommendation = recommend_df
        return self

    def getPrediction(self):
        return self._recommendation




