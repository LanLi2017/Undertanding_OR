import csv
import os
from datetime import datetime
from collections import Counter
from pprint import pprint

import numpy as np
import pandas as pd


class Operation:
    # decompose operations into basic ones

    def __init__(self, D):
        '''
        D: DataFrame
        ywfile_p: yw file path
        data_p: inter-product path
        '''
        self.D = D # initialize new data frame

        # initialize previous data path
        # self.prev_data_p = data_p

        # yw file path
        # self.yw = ywfile_p

        # count data cleaning steps
        # self.counter = 0
        # dependency list for creating dependency tree
        self.dependency = list()

    # def pd_csv(self,data_p):
    #     self.D = pd.read_csv(data_p)

    def row_idx_change(self,col_name, expression):
        # return the changed cell with row index
        for index, value in self.D[col_name].items():
            if value != value.expression():
                return index

    # row level
    def del_row(self, row_idx):
        '''
        # copy row (value + position)
        delete row (position)
        # add row (position)
        input dataset
        :return: output dataset/ stringIO / List
        '''
        # self.counter += 1
        # self.yw.write(f"#@begin delete-row @desc delete row {row_idx}\n")
        # self.yw.write(f"#@param row:{row_idx}\n")
        # self.yw.write("#@param Symbol:-\n")
        self.D = self.D.drop(row_idx)
        # self.yw.write(f"#@in wf_step:{self.counter}\n")
        # self.yw.write(f"#@in {self.prev_data_p}\n")
        #
        # # current data_p
        # current_data_p = f'temp_out/del_row_op_{row_idx}_step_{self.counter}.csv'
        # # save the temporary outputs
        # self.save_temp(current_data_p)
        #
        # self.yw.write(f"#@out {current_data_p}\n")

        record = { 'row': f'- {row_idx}'}
        self.dependency.append(record)

        # self.yw.write("#@end delete-row\n")
        #
        # # update previous path
        # self.prev_data_p = current_data_p
        return self.D

    # column level
    def del_col(self,drop_col):
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
        self.D = self.D.drop(drop_col,axis=1)
        record = {'column': f'- {drop_col}'}
        self.dependency.append(record)
        return self.D

    def add_col(self,new_col,old_col,insert_idx,copy=True,*add_col_val):
        # new column name
        # old column name
        # new column position
        print("#@begin add column")
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

    def move_col(self,insert_idx,old_idx,new_col,old_col):
        # insert_idx: move the column to new position
        # old_idx: delete the old position column
        # new_col: new name for moving column
        # old_col: old position column name
        # copy add
        self.add_col(new_col,old_col,insert_idx,copy=True)
        # delete
        self.del_col(old_idx)

    def split_col(self,old_col,regex=',',keep_old=True):
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
            self.del_col(old_col)
            record1 = {'column': f'-{old_col}'}
            self.dependency.append(record1)
            self.D = pd.concat([self.D, new_df], axis=1)

    def rename_col(self,new_name,old_col,old_col_idx):
        # table + add/copy column(new name,same position) + delete old column
        self.add_col(new_name,old_col,old_col_idx,copy=True)
        self.del_col(old_col)

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

    def split_simplify(self):
        # 对于任何一个operation， 首先进行拆分简化，通过排序来比较路径是否相等
        # 任何OPERATION调用他以后都能得到一个简化版的细分路径/BASE OPS
        # reverse narrow: 哪些操作是相反的
        # column index: 这俩是不是作用在同一个DOMAIN
        # 动态规划； 概率方式
        # add minus

        pass

    def topology_(self):
        pass


class DependencyTree:
    def __init__(self):
        pass


class YW(Operation):
    def __init__(self,  D, ywfile_p, data_p, dir):

        # D_0 : prev dataframe
        # ywfile_p : yw file path
        # data_p : inter-product path
        super().__init__(D)

        self. D_0 = D

        # initialize previous data path
        self.prev_data_p = data_p

        # yw file path
        self.yw = ywfile_p

        # count data cleaning steps
        self.counter = 0

        self.dir = dir

    def save_temp(self, data_p):
        # save the temporary inter-products
        self.D_0.to_csv(f'{data_p}.csv', index=False)

    def del_row(self, row_idx):
        self.counter += 1
        self.yw.write(f"#@begin delete-row @desc delete row {row_idx}\n")
        self.yw.write(f"#@param row:{row_idx}\n")
        self.yw.write("#@param Symbol:-\n")
        self.D_0 = super().del_row(row_idx)
        self.yw.write(f"#@param wf_step:{self.counter}\n")
        self.yw.write(f"#@in {self.prev_data_p}\n")

        # current data_p
        current_data_p = f'{self.dir}/del_row_{row_idx}_step_{self.counter}.csv'
        # save the temporary outputs
        self.save_temp(current_data_p)

        self.yw.write(f"#@out {current_data_p}\n")
        self.yw.write("#@end delete-row\n")

        # update previous path
        self.prev_data_p = current_data_p
        return self.D_0

    def del_col(self,drop_col):
        self.counter += 1
        self.yw.write(f"#@begin delete-column @desc delete column {drop_col}\n")
        self.yw.write(f"#@param column:{drop_col}\n")
        self.yw.write("#@param Symbol:-\n")
        self.D_0 = super().del_col(drop_col=drop_col)
        self.yw.write(f"#@param wf_step:{self.counter}\n")
        self.yw.write(f"#@in {self.prev_data_p}\n")

        # current data_p
        current_data_p = f'{self.dir}/del_col_{drop_col}_step_{self.counter}.csv'
        # save the temporary outputs
        self.save_temp(current_data_p)

        self.yw.write(f"#@out {current_data_p}\n")
        self.yw.write("#@end delete-column\n")

        # update previous path
        self.prev_data_p = current_data_p
        return self.D_0


def main():
    data_p='Data_input.csv'
    D = pd.read_csv(data_p,index_col=0)

    # create different output packages according to time stamps
    now = datetime.now()
    # time stamp: month_day_hour_minute
    timetag = now.strftime("%m_%d_%H_%M")
    # top directory for storing data cleaning steps
    dir_dc_steps = f'temp_out_{timetag}'
    os.makedirs(f"{dir_dc_steps}", exist_ok=True)

    # final yw file dir
    ywfile_out = f'yw_out_{timetag}'
    os.makedirs(f"{ywfile_out}", exist_ok=True)

    # column name!!!
    with open(f'{ywfile_out}/yw.txt', 'w')as f:
        yw = YW(D, f, data_p,dir_dc_steps)
        yw.del_row(2)
        yw.del_col("amount")


if __name__=='__main__':
    main()