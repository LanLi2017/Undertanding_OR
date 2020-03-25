import csv
from collections import Counter

import numpy as np
import pandas as pd


class Operation:
    # decompose operations into basic ones

    def __init__(self):
        self.D=pd.DataFrame() # initialize new data frame
        self.dependency=list()

    def pd_csv(self,data_p):
        self.D = pd.read_csv(data_p)

    def row_idx_change(self,col_name, expression):
        # return the changed cell with row index
        for index, value in self.D[col_name].items():
            if value != value.expression():
                return index

    # row level
    def base_del_row_op(self,row_idx):
        '''
        # copy row (value + position)
        delete row (position)
        # add row (position)
        input dataset
        :return: output dataset/ stringIO / List
        '''
        self.D = self.D.drop(row_idx)
        record = { 'row': f'- {row_idx}'}
        self.dependency.append(record)

    # column level
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
        record = {'column': f'- {drop_col}'}
        self.dependency.append(record)

    def base_add_col_op(self,new_col,old_col,insert_idx,copy=True,*add_col_val):
        # new column name
        # old column name
        # new column position
        record = {'column': f'+ {new_col}'}
        self.dependency.append(record)
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
        for col in new_df.columns:
            record = {'column':f'+ {col}'}
            self.dependency.append(record)
        if keep_old:
            # the whole table + new generated columns
            self.D = pd.concat([self.D, new_df],axis=1)
            # self.base_add_col_op()
            # self.D[]=self.D[]
        else:
            # the whole table - old_col +new generated columns
            self.base_del_col_op(old_col)
            record1 = {'column': f'-{old_col}'}
            self.dependency.append(record1)
            self.D = pd.concat([self.D, new_df], axis=1)

    def rename_col_op(self,new_name,old_col,old_col_idx):
        # table + add/copy column(new name,same position) + delete old column
        self.base_add_col_op(new_name,old_col,old_col_idx,copy=True)
        self.base_del_col_op(old_col)

    # Cell level
    def cell_mass_edit(self,expression,col_name):
        '''
        expression: apply function
        col_name: apply column
        row_idx: changed row
        dependency: row_index, column_name
        '''
        # cluster-edit
        # common transformations
        if expression == 'uppercase':
            for index, value in self.D[col_name].items():
                if value == value.upper():
                    pass
                else:
                    record = {'cell': (index, col_name)}
                    self.dependency.append(record)
                    return index
        elif expression == 'lowercase':
            for index, value in self.D[col_name].items():
                if value == value.lower():
                    # no change
                    pass
                else:
                    record = {'cell': (index, col_name)}
                    self.dependency.append(record)
                    return index
        elif expression == 'number':
            # here use double
            for index, value in self.D[col_name].items():
                try:
                    if value == int(value):
                        pass
                    else:
                        record = {'cell': (index, col_name)}
                        self.dependency.append(record)
                        return index
                except ValueError:
                    pass

    def cell_single_edit(self,row_idx, col_name):
        record = {'cell': (row_idx, col_name)}
        self.dependency.append(record)

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

    def output_data(self, name):
        self.D.to_csv(f'{name}.csv', index=False)
        return self.D


def main():
    data_p='temp_table.csv'
    OPS1=Operation()
    OPS1.pd_csv(data_p)
    OPS1.base_del_row_op(2)
    OPS1.base_add_col_op('color_style_copy','color_style',3)
    OPS1.split_col_op('color_style_copy','_')
    OPS1.base_del_col_op('color_style_copy')
    OPS1.rename_col_op('uID','id',0)
    # df1=OPS1.output_data('OPS1')
    print(OPS1.dependency)

    '''
    op_list1: 
    1. drop row 2
    2. copy column "color_style" , new name "color_style_copy"
    3. split column "color_style_copy", regex= "_", keep_origin= True
    4. del column "color_style_copy"
    5. rename column "id" to "uID"
    
    # '''
    # OPS2=Operation()
    # OPS2.pd_csv(data_p)
    # OPS2.rename_col_op('uID','id',0)
    # OPS2.base_add_col_op('color_style_copy','color_style',3)
    # OPS2.base_del_row_op(2)
    # OPS2.split_col_op('color_style_copy','_',keep_old=False)
    #
    # df2=OPS2.output_data('OPS2')

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
    # print(df1.equals(df2))


if __name__=='__main__':
    main()