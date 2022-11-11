import numpy as np
import pandas as pd
import pickle
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

data = pd.read_csv('./clean.csv',
                   names=['out_count', 'in_count', 'out_addr_count', 'in_addr_count', 'out_avg', 'in_avg', 'gap_time',
                          'label'])

feature_columns = ['out_count', 'in_count', 'out_addr_count', 'in_addr_count', 'out_avg', 'in_avg', 'gap_time']
target_column = 'label'

train, test = train_test_split(data, test_size=0.3, random_state=23)
xgtrain = xgb.DMatrix(train[feature_columns].values, train[target_column].values)
xgtest = xgb.DMatrix(test[feature_columns].values, test[target_column].values)

# base param
param = {'max_depth': 5,
         'eta': 0.1,
         'subsample': 0.7,
         'colsample_bytree': 0.7,
         'objective': 'multi:softmax',
         'num_class': 3}

watchlist = [(xgtest, 'eval'), (xgtrain, 'train')]
num_round = 40
bst = xgb.train(param, xgtrain, num_round, watchlist)
preds = bst.predict(xgtest)

print('Accuracy of prediction:', accuracy_score(test[target_column].values, preds))

bst.save_model('./boost.model')

# predict test

data = pd.read_csv('./unknown_label.csv',
                   names=['address', 'out_count', 'in_count', 'out_addr_count', 'in_addr_count', 'out_avg', 'in_avg',
                          'gap_time'])
feature_columns = ['out_count', 'in_count', 'out_addr_count', 'in_addr_count', 'out_avg', 'in_avg', 'gap_time']
predict_data = xgb.DMatrix(data[feature_columns].values)
address = data['address']

for index, s in enumerate(predict_data):
    if s == 0:
        print('hot')
        print(address[index])
    elif s == 1:
        print(address[index])
