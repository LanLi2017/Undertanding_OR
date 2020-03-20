import csv
from collections import Counter

import numpy as np
import pandas as pd


class cell:
    def __init__(self):
        pass


    pass


class Operation:
    # 定义了什么操作：传入一个数据集， 通过这个大函数 能得到另外一个数据集
    # 对于任意一个OPERATION集合路径，都可以调用 split-simplify-.py
    # 有几个OPERATION就写几个函数； 而且这里的OPS都是大OP（需要细分）, 必须要调用BASE OP
    # 细分路径 -> BASE_OP

    # OP1 -> OP2 -> OP3 (ori)
    # OP1 -> OP3 -> OP2
    # OP2 -> OP1 -> OP3  (OP1 AND OP3 dependency)
    # enumeration (ALL relationship of transformations)-> EQUAL (路径)
    # FUNC: 判断是否是相同效果路径
    # 1. INPUT 数据集+路径--》 看结果判断是否相同
    # 优化： 作弊：数据本身的关系/
    # 1.解决方法： 虚拟数据集（10个）来判断： 减少因为真实数据本身产生问题； 减少数据的大小
    # 缺点： 和真实数据之间的区别造成转化不能操作，不能完全模拟
    # 2.解决方法： 简化排序： 提出所有小操作，然后增加； OP1 和 OP2, 分别拆分（）和排序（比如COLUMN INDEX）
    # (1,3) (2)
    # OP1 / OP2 / OP3
    # BASE OP （要么对行， 要么对列， 要么对整体 （乘法），第I,J先不管）: 其他OP基于BASE OP

    def __init__(self):
        self.D=pd.DataFrame() # initialize new data frame

    def pd_csv(self,data_p):
        self.D = pd.read_csv(data_p)

    def base_del_row_op(self,row_idx):
        '''
        # copy row (value + position)
        delete row (position)
        # add row (position)
        input dataset
        :return: output dataset/ stringIO / List
        '''
        self.D = self.D.drop(row_idx)

    def base_del_col_op(self,drop_col):
        ''' A (COUNT) B () C ()
        A:3 B:5 C:7
        质数相加，唯一性？
        add column
        # copy column
        delete column
        # rename column
        # inter-column: date format/ number/ text...
        # move column: copy + position & delete
        :return:
        '''
        self.D = self.D.drop(columns=drop_col)

    def base_add_col_op(self,new_col,old_col,insert_idx,copy=True,*add_col_val):
        # new column name
        # old column name
        # new column position
        if copy:
            # copy the value from the old column
            new_col_val=self.D[old_col]
            self.D.insert(loc=insert_idx, column=new_col, value=new_col_val)
        else:
            # new values based on the old column
            for arg in add_col_val:
                self.D.insert(loc=insert_idx,column=new_col, value=arg)

    def move_col_op(self,insert_idx,old_idx,new_col,old_col):
        # insert_idx: move the column to new position
        # old_idx: delete the old position column
        # new_col: new name for moving column
        # old_col: old position column name
        # copy add
        self.base_add_col_op(new_col,old_col,insert_idx,copy=True)
        # delete
        self.base_del_col_op(old_idx)

    def split_col_op(self,old_col,regex=',',keep_old=True):
        # split
        new_df = pd.concat(self.D[old_col].str.split(regex, expand=True), axis=1, keys=self.D.columns)
        new_df.columns = new_df.columns.map(lambda x: '_'.join((x[0], str(x[1]))))
        new_df = new_df.replace({'': np.nan, None: np.nan})
        if keep_old:
            # the whole table + new generated columns
            self.D = pd.concat([self.D, new_df],axis=1)
            # self.base_add_col_op()
            # self.D[]=self.D[]
        else:
            # the whole table - old_col +new generated columns
            self.base_del_col_op(old_col)
            self.D = pd.concat([self.D, new_df], axis=1)

    def rename_col_op(self):
        pass

    def cell_pos(self):
        # cell index
        pass

    def split_simplify_op(self):
        # 对于任何一个operation， 首先进行拆分简化，通过排序来比较路径是否相等
        # 任何OPERATION调用他以后都能得到一个简化版的细分路径/BASE OPS
        # reverse narrow: 哪些操作是相反的
        # column index: 这俩是不是作用在同一个DOMAIN
        # 动态规划； 概率方式
        # add minus

        pass

    def topology_(self):
        pass


class DTA:
    '''
    存储数据，只做内部转换
    函数写外面，2个DTA??
    发现外部过于Messy
    '''
    def __init__(self):
        # 传入数据集，
        # five parameters for dataset
        self.R= dict() # regular expression for each column
        self.L=Counter() # (value, count)
        self.I=set() # row set
        self.J=set() # column set
        self.S={} # structuring func: content -> row&col indices
        self.data_space=set() # current dataset

    def data_regex(self,data_p):

        return self.R

    def data_content(self,data_p):
        data = []
        with open(data_p, 'r')as csvfile:
            csvreader = csv.reader(csvfile)
            next(csvreader) # remove header
            for row in csvreader:
                data += row
        '''
        L:  {(value,frequency),...}
        series.value_counts
        '''
        self.L=Counter(data)

    def row_column_index(self,data_p):
        df=pd.read_csv(data_p)
        row_length=df.shape[0]
        column_length=df.shape[1]
        self.I=set(range(row_length))
        self.J=set(range(column_length))

    def structuring_func(self,data_p):
        # S: {c -> (i,j)}
        # self.S = np.cross(self.I, self.J)
        data = np.genfromtxt(data_p, dtype=None, delimiter=',', encoding='utf-8')
        print(data)
        for index, x in np.ndenumerate(data):
            self.S[x] = index

    def dataset_space(self):
        self.D = (self.R, self.L, self.S, self.I, self.J)


class Toolkit:
    def __init__(self):
        pass

    def signature_set(self,prev_data_sp, cur_data_sp):
        # return difference in data spaces of ops-
        # prev_data_sp: previous data space
        # prev_dsp_list: previous data space list
        # cur_data_sp: current data space
        # cur_dsp_list: current data space list
        # sig_set: signature set, unifying data space states changes from previous to current
        sig_set = []
        prev_dsp_list = []
        cur_dsp_list = []
        for i in range(len(prev_data_sp)):
            # pair = []
            if prev_data_sp[i] != cur_data_sp[i]:
                prev_dsp_list.append(prev_data_sp[i])
                cur_dsp_list.append(cur_data_sp[i])
        sig_set += (prev_dsp_list, cur_dsp_list)

        return sig_set

    def shallow_trans(self,sig_set):
        '''
        input signature set
        :return: boolean
        '''

        pass

    def deep_trans(self):
        pass

    def identity_trans(self):
        pass

    def equal(self):
        # two data spaces are equal
        pass


def main():
    data_p=''
    OPS1=Operation()
    '''
    op_list1: 
    1. drop row 2
    2. copy column "color_style" , new name "color_style_copy"
    3. split column "color_style_copy", regex= "_", keep_origin= True
    4. del column "color_style_copy"
    
    '''
    '''
    op_list2:
    1. copy column "color_style" , new name "color_style_copy"
    2. drop row 2
    3. split column "color_style_copy", regex= "_", keep_origin = False

    '''
    '''
    op1 transfer:
    1. drop row 2 : table - row_2
    2. add column "color_style_copy" : table - row_2 + col_"color_style_copy"
    3. table - row_2 + col_"color_style_copy" + new_generated_columns
    4. table - row_2 + col_"color_style_copy" + new_generated_columns - col_"color_style_copy"
    
    op2 transfer:
    1. add column "color_style_copy" : table + col_"color_style_copy"
    2. drop row 2:  table + col_"color_style_copy" - row_2
    3.  table + col_"color_style_copy" - row_2 + (new_generated_columns - col_"color_style_copy")
    
    Q & A
    table - row_2 + col_"color_style_copy" + new_generated_columns - col_"color_style_copy"
    equal to 
    table + col_"color_style_copy" - row_2 + (new_generated_columns - col_"color_style_copy")
    ?
    
    '''


    pass


if __name__=='__main__':
    main()