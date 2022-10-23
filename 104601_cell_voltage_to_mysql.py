from data_cleaning.Module_fat_internal_report import GetFileList
from data_cleaning.Module_fat_internal_report import FATInternalReport
from data_cleaning.Module_fat_internal_report import StackNumber
import pandas as pd
from sqlalchemy import create_engine


# define variables to save directory
dir_104601 = r"D:\ShareCache\上海韵量新能源科技有限公司\产品技术部PTD\PTD China\Testing\4. Production FAT Reports\Electra 1.0 211E with Dummy cells\2. FAT internal reports"
# call function to get the full name of file in the directory
list_csv_fullname = GetFileList(dir_104601).get_csv_files()

# call function to get the cleaned data
cell_voltage = pd.DataFrame()
for i in list_csv_fullname:
    try:
        data = FATInternalReport(csv_directory=i).cell_voltage()
        cell_voltage = pd.concat([data, cell_voltage], axis=0)
    except:
        print("failed_file: " + i.split("\\")[-1])

# reset index of DataFrame
cell_voltage.reset_index(inplace=True)
# drop original index
cell_voltage.drop(columns=["index"], inplace=True)

# call function to clean data
new_stack_number = []
stack_category = []

# ergodic index of DataFrame
for i in cell_voltage.index.to_list():
    # create one instance based on object
    stack_information = StackNumber(part_number="104601",
                                    old_stack_number=cell_voltage.iloc[i]["stack_number"],
                                    cell_count=cell_voltage.iloc[i]["cell_count"])

    # call function to clean data
    new_stack = stack_information.new_stack_number()
    tag = stack_information.stack_category()

    # append to list
    new_stack_number.append(new_stack)
    stack_category.append(tag)

# update DataFrame with cleaned data
cell_voltage["stack_number"] = new_stack_number
cell_voltage["stack_category"] = stack_category
cell_voltage["part_number"] = "104601"

# transfer string to datetime
cell_voltage["test_date_time"] = pd.to_datetime(cell_voltage["test_date_time"])

# create new DataFrame
dim_test_history = cell_voltage.iloc[:][["stack_number", "test_date_time"]]
dim_test_history.drop_duplicates(inplace=True)

# create new column for newly created DataFrame
dim_test_history["test_times"] = dim_test_history.groupby("stack_number")["test_date_time"].rank(method="min", ascending=True)

# concatenate DataFrame
df_merged = pd.merge(left=cell_voltage,
                     right=dim_test_history[["test_date_time", "test_times"]],
                     left_on="test_date_time",
                     right_on="test_date_time")

# sort values
df_merged.sort_values(by=["test_date_time"], ascending=True, inplace=True)

# create connection to MySQL
connection = create_engine("mysql+pymysql://root:Hjl19910615@localhost:3306/process_quality")
if connection:
    print("connection successfully !!")

# data save into MySQl
df_merged.to_sql(name="104601_fat_cell_voltage",
                 con=connection,
                 if_exists="replace",
                 index=False)
