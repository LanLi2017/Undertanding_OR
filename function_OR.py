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
        new_df = pd.concat([self.D[old_col].str.split(regex, expand=True)], axis=1,keys=[old_col])
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

    def rename_col_op(self,new_name,old_col,old_col_idx):
        # table + add/copy column(new name,same position) + delete old column
        self.base_add_col_op(new_name,old_col,old_col_idx,copy=True)
        self.base_del_col_op(old_col)

    def output_data(self,name):
        self.D.to_csv(f'{name}.csv',index=False)
        return self.D

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


def main():
    data_p='temp_table.csv'
    OPS1=Operation()
    OPS1.pd_csv(data_p)
    OPS1.base_del_row_op(2)
    OPS1.base_add_col_op('color_style_copy','color_style',3)
    OPS1.split_col_op('color_style_copy','_')
    OPS1.base_del_col_op('color_style_copy')
    OPS1.rename_col_op('uID','id',0)
    df1=OPS1.output_data('OPS1')

    '''
    op_list1: 
    1. drop row 2
    2. copy column "color_style" , new name "color_style_copy"
    3. split column "color_style_copy", regex= "_", keep_origin= True
    4. del column "color_style_copy"
    5. rename column "id" to "uID"
    
    '''
    OPS2=Operation()
    OPS2.pd_csv(data_p)
    OPS2.rename_col_op('uID','id',0)
    OPS2.base_add_col_op('color_style_copy','color_style',3)
    OPS2.base_del_row_op(2)
    OPS2.split_col_op('color_style_copy','_',keep_old=False)

    df2=OPS2.output_data('OPS2')

    '''
    op_list2:
    1. rename "id" to "uID"
    2. copy column "color_style" , new name "color_style_copy"
    3. drop row 2
    4. split column "color_style_copy", regex= "_", keep_origin = False

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
    print(df1.equals(df2))


if __name__=='__main__':
    main()