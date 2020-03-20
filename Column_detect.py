
# tuple1 = (0 ,1, 2, 3)
# tuple2=('babba','ddd')
# print(tuple1)
import csv
from collections import Counter

import pandas as pd
import numpy as np
# I=(int(9))
# print(I)
# J=(0,1,2)
# df=pd.read_csv('temp_table.csv')
# df_shape=df.shape
# print(type(df_shape))
# (8,3)
# I=df_shape[0]
# J=df_shape[1]
# print(I)
# for i in range(1,I+1):
#     for value,count in enumerate(df[i]):
#         print(value,':',count)
# print(df['name'].value_counts())
# data=[]
# with open('temp_table.csv','r')as csvfile:
#     csvreader = csv.reader(csvfile)
#     next(csvreader)
#     for row in csvreader:
#         data+=row
# print(Counter(data))
# i=9
# I=set(range(i))
# print(I)
# R : 列的正则表达式
# S: 这个数据的结构
# I : 这个数据的行
# J： 这个数据的列
# L: 数据的内容： '1'： 100； 'SUN': 200; ....
# DATA SPACE ： DTA
set1= ('R','S','I','J')
set2 = ('R\'','S','I\'','J')
# or_set=set()
# new_set=set()
# result=[]
pair=[]
or_sp_list=[]
new_sp_list=[]
for i in range(len(set1)):
    # pair = []
    if set1[i]!=set2[i]:
        or_sp_list.append(set1[i])
        new_sp_list.append(set2[i])

        # result.append(pair)
    # else:
    #     pass

    # result.append(pair)
pair+=(or_sp_list,new_sp_list)
print(pair)
#
# def myFun(arg1, *new_col_value):
#     print('first argument :', arg1)
#     print('next argument through *argx:', new_col_value)
#
# myFun('hello',[1,2,3,4])

# df = pd.DataFrame([['', 'mariachi', 'mexico, united states'],
#                    ['', 'jazz, rap', 'united states'],
#                    ['', '', 'spain'],
#                    ['jimi hendrix, john lennon', 'rock', ''],
#                    ['spirit', '', 'united states'],
#                    ['', 'latin', 'united states'],
#                    ['', '', ''],
#                    ['speak', '', 'mexico, united states']],
#                    columns=['Musician', 'Genre', 'Country'])
# cols=['Musician', 'Genre', 'Country']
# df=pd.concat([df[x].str.split(',', expand=True)for x in cols], axis=1, keys=df.columns)
# df.columns = df.columns.map(lambda x: '_'.join((x[0], str(x[1]))))
# df = df.replace({'':np.nan, None: np.nan})
# print(df)
#




