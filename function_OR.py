import os
from datetime import datetime

import numpy as np
import pandas as pd


class Operation:
    # decompose operations into basic ones

    def __init__(self, D):
        '''
        D: DataFrame
        ywfile_p: Yesworkflow file path
        data_p: inter-product path
        '''
        self.D = D # initialize new data frame

        self.dependency = list()

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
        self.D = self.D.drop(row_idx)

        record = { 'row': f'- {row_idx}'}
        self.dependency.append(record)
        return self.D

    # column level
    def del_col(self,drop_col):
        '''
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
        return self.D

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

    def rename_col(self,new_name,old_col, insert_col_idx):
        # table + add/copy column(new name,same position) + delete old column
        self.add_col(new_name,old_col,insert_col_idx,copy=True)
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

    def output_data(self, output_p):
        self.D.to_csv(output_p)


class DependencyTree:
    def __init__(self):
        pass


class YW(Operation):
    def __init__(self,  D, ywfile_p, data_p, dir):

        # D_0 : prev dataframe
        # ywfile_p : Yesworkflow file path
        # data_p : inter-product path
        super().__init__(D)

        self. D_0 = D

        # initialize previous data path
        self.prev_data_p = data_p

        # Yesworkflow file path
        self.yw = ywfile_p

        # count data cleaning steps
        self.counter = 0

        self.dir = dir

        # #@params : list
        self.params =list()
        # #@in : list
        self.data_in = list()

    def save_temp(self, data_p):
        # save the temporary inter-products
        self.D_0.to_csv(f'{data_p}.csv', index=False)

    def del_row(self, row_idx):
        self.counter += 1

        params1 = f"#@param row:{row_idx}\n"
        params2 = f"#@param Symbol:-\n"
        params3 = f"#@param wf_step:{self.counter}\n"
        # this is for head of yw file
        self.params.extend([params1,params2,params3])

        self.yw.write(f"#@begin delete-row @desc delete row {row_idx}\n")
        self.yw.write(params1)
        self.yw.write(params2)
        self.D_0 = super().del_row(row_idx)
        self.yw.write(params3)

        self.yw.write(f"#@in {self.prev_data_p}\n")

        # current data_p
        current_data_p = f'{self.dir}/del_row_{row_idx}_step_{self.counter}.csv'
        # save the temporary outputs
        self.save_temp(current_data_p)

        self.yw.write(f"#@out {current_data_p}\n")
        self.yw.write("#@end delete-row\n")

        # update previous path
        self.prev_data_p = current_data_p
        self.data_in = current_data_p
        return self.D_0

    def add_col(self,new_col,old_col,insert_idx,copy=True,*add_col_val):
        self.counter += 1

        params1 = f"#@param NewColumn:{new_col}\n"
        params2 = f"#@param OldColumn:{old_col}\n"
        params3 = f"#@param InsertColumnIndex:{insert_idx}\n"
        params4 = f"#@param wf_step:{self.counter}\n"
        self.params.extend([params1, params2, params3, params4])

        self.yw.write(f"#@begin add-column @desc add column {new_col}\n")
        self.yw.write(params1)
        self.yw.write(params2)
        self.yw.write(params3)
        self.D_0 = super().add_col(new_col,old_col,insert_idx,copy,*add_col_val)
        self.yw.write(params4)

        self.yw.write(f"#@in {self.prev_data_p}\n")

        # current data_p
        current_data_p = f'{self.dir}/add_column_{new_col}_step_{self.counter}.csv'
        # save the temporary outputs
        self.save_temp(current_data_p)

        self.yw.write(f"#@out {current_data_p}\n")
        self.yw.write("#@end add-column\n")

        # update previous path
        self.prev_data_p = current_data_p
        self.data_in = current_data_p
        return self.D_0

    def del_col(self,drop_col):
        self.counter += 1

        params1 = f"#@param column:{drop_col}\n"
        params2 = "#@param Symbol:-\n"
        params3 = f"#@param wf_step:{self.counter}\n"
        self.params.extend([params1, params2, params3])

        self.yw.write(f"#@begin delete-column @desc delete column {drop_col}\n")
        self.yw.write(params1)
        self.yw.write(params2)
        self.D_0 = super().del_col(drop_col=drop_col)
        self.yw.write(params3)
        self.yw.write(f"#@in {self.prev_data_p}\n")

        # current data_p
        current_data_p = f'{self.dir}/del_col_{drop_col}_step_{self.counter}.csv'
        # save the temporary outputs
        self.save_temp(current_data_p)

        self.yw.write(f"#@out {current_data_p}\n")
        self.yw.write("#@end delete-column\n")

        # update previous path
        self.prev_data_p = current_data_p
        self.data_in = current_data_p
        return self.D_0

    def rename_col(self,new_name,old_col,insert_col_idx):
        params1 = f"#@param NewColumn:{new_name}\n"
        params2 = f"#@param OldColumn:{old_col}\n"
        params3 = f"#@param InsertColumnIndex:{insert_col_idx}\n"
        params4 = f"#@param wf_step:{self.counter}\n"
        self.params.extend([params1,params2,params3,params4])

        self.yw.write(f"#@begin rename-column @desc rename column {old_col} into {new_name}\n")
        self.yw.write(params1)
        self.yw.write(params2)
        self.yw.write(params3)
        # copy
        self.D_0 = super().rename_col(new_name,old_col,insert_col_idx)
        # self.D_0 = self.del_col(old_col)
        self.yw.write(params4)

        self.yw.write(f"#@in {self.prev_data_p}\n")

        # current data_p
        current_data_p = f'{self.dir}/rename_{old_col}2{new_name}_step_{self.counter}.csv'
        # save the temporary outputs
        self.save_temp(current_data_p)

        self.yw.write(f"#@out {current_data_p}\n")
        self.yw.write("#@end rename-column\n")

        # update previous path
        self.prev_data_p = current_data_p
        self.data_in = current_data_p
        return self.D_0


def main():
    # INPUT DATA PATH
    data_p='Data_input.csv'
    D = pd.read_csv(data_p,index_col=0)

    # create different output packages WITH time stamps
    now = datetime.now()
    # time stamp: month_day_hour_minute
    timetag = now.strftime("%m_%d_%H_%M")
    # top directory for storing data cleaning steps
    dir_dc_steps = f'temp_out_{timetag}'
    os.makedirs(f"{dir_dc_steps}", exist_ok=True)

    # Yesworkflow file dir
    ywfile_out = f'yw_out_{timetag}'
    os.makedirs(f"{ywfile_out}", exist_ok=True)

    # column name!!!
    # transformation part
    with open(f'{ywfile_out}/yw_func.txt', 'w')as f:
        # f.write("#@begin Hybrid_Prov_model @desc hybrid provenance model for understanding OpenRefine transformation\n")
        # f.write("")
        yw = YW(D, f, data_p,dir_dc_steps)
        yw.del_row(2)
        yw.del_col("amount")
        yw.rename_col("fruit","name",0)
    print(yw.params)
    params_list = yw.params
    out = yw.data_in
    print(out)

    # wf_head + transformation part
    with open(f'{ywfile_out}/yw.txt', 'w')as f1, open(f'{ywfile_out}/yw_func.txt', 'r')as f2:
        f1.write("#@begin Hybrid_Prov_model @desc hybrid provenance model for understanding OpenRefine transformation\n")
        for param in params_list:
            f1.write(param)
        f1.write(f"#@in {data_p}\n")
        f1.write(f"#@out {out}\n")
        for line in f2:
            f1.write(line)
        f1.write("#@end Hybrid_Prov_model")


def main2():
    # INPUT DATA PATH
    data_p = 'Data_input.csv'
    output_p ='Data_output.csv'
    D = pd.read_csv(data_p, index_col=0)
    op = Operation(D)
    op.rename_col("fruit","name",2)
    op.output_data(output_p)


if __name__ == '__main__':
    main()