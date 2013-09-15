"""
Entry of experiment

Just run from command line: Python pipeline.py

It will run the baseline model and print the recommendation dataframe (top 5 users for test), you could set the test user
amount to be any positive number by changing the "top_n_users" attribute in model.py. To generate all users'
recommendation, set the "_TEST" attribute in model.py to be 0 -- be cautious it could be very slow

The default recommendations are 10 songs. If you want to get more, pass an argument "max_recommendations=n" when
creating the model. "n" could be any positive numbers -- be cautious that there are millions of songs.

The prediction of recommendation would look like this:

                                                           0                   1                   2                   3                   4
00007a02388c208ea7176479f6ae06f8224355b3  SOBONKR12A58A7A7E0  SOAUWYT12A81C206F1  SOSXLTC12AF72A7F54  SOFRQTD12A81C233C0  SOEGIYH12A6D4FC0E3
00014a76ed063e1a749171a253bca9d9a0ff1782  SOBONKR12A58A7A7E0  SOAUWYT12A81C206F1  SOSXLTC12AF72A7F54  SOFRQTD12A81C233C0  SOEGIYH12A6D4FC0E3
00015189668691680bb1a2e58afde1541ec92ced  SOBONKR12A58A7A7E0  SOAUWYT12A81C206F1  SOSXLTC12AF72A7F54  SOFRQTD12A81C233C0  SOEGIYH12A6D4FC0E3
0001ff7aa2667c8d8b945317b88adaed1c0b9dc2  SOBONKR12A58A7A7E0  SOAUWYT12A81C206F1  SOSXLTC12AF72A7F54  SOFRQTD12A81C233C0  SOEGIYH12A6D4FC0E3
00020fcd8b01986a6a85b896ccde6c49f35142ad  SOBONKR12A58A7A7E0  SOAUWYT12A81C206F1  SOSXLTC12AF72A7F54  SOFRQTD12A81C233C0  SOEGIYH12A6D4FC0E3


First column: userID
Second and the rest columsn: sorted song IDs
"""

__author__ = 'james'

import sys
import importer
import model


class Pipeline:
    def __init__(self, train_filename=None, test_filename=None, model=None):
        try:
            self._data_train = importer.import_dataset(train_filename)
            self._data_test = importer.import_dataset(test_filename)
            self._model = model
        except Exception:
            print train_filename
            print test_filename
            sys.exit(1)

    def setModel(self, model):
        self._model = model
        print "finished setModel"
        return self

    def runModel(self):
        self._model.train(self._data_train)
        self._model.predict(self._data_test)
        print "finished runModel"
        return self

    def getPrediction(self):
        return self._model.getPrediction()

    def printPrediction(self):
        print self._model.getPrediction().head()


if __name__ == '__main__':
    """
    A simple test case for the baseline model
    """
    train_filename = '../data/kaggle_visible_evaluation_triplets.txt'
    test_filename = '../data/kaggle_visible_evaluation_triplets.txt'
    baseline_model = model.Model(max_recommendations=5)
    baseline = Pipeline(train_filename, test_filename)
    baseline.setModel(baseline_model)
    baseline.runModel()
    baseline.printPrediction()

