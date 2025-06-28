import numpy as np
import pandas as pd
from RD_Cleaning_Data_Code import nls_file_path
import matplotlib.pyplot as plt


try:
    nls_data = pd.read_csv(nls_file_path)
except FileNotFoundError:
    print(f"Error: The file {nls_file_path} was not found. Please check the file path.")
    raise
nls_data = pd.DataFrame(nls_data)
# Display the first few rows of the data to understand its structure
print("Loaded data. Here are the first few rows:")
print(nls_data.head())

# Renaming child ID column
nls_data.rename(columns={'CPUBID_XRND': 'id'}, inplace=True)



age_data = pd.read_csv("data-preprocessing/Initial_Preprocessing/child_age_panel_BEST.csv")

# Seeing how many people answer the "headstart questions"
for column in ["CHILD_EVER_ENROLLED_IN_HEAD", "HOW_LONG_CHILD_WAS_IN_HEAD", "CHILD_AGE_WHEN_1ST_ATTD_HEA"]: 
    count = age_data.count()[column]
    total = age_data.count()["id"]
    print(f"Column {column} has count {count}, being answered for {count/total}")

column_to_examine = "MOM_HELPS_CH_LEARN_NUMBERS"
age_to_examine = 6
column_nls = "HOME_PART_B_MOM_HELPS_CH_LEARN_NUMBERS"
keep_columns = ["id", "year", "CYRB_XRND", column_to_examine]
age_dist = age_data.loc[(age_data[column_to_examine] > 0), "age"]
plt.hist(age_dist, bins=np.arange(15)+0.5)
plt.show()

weird_data = age_data.loc[(age_data["age"] > age_to_examine) & (age_data[column_to_examine] > 0)]
weird_data_ages = weird_data["age"].unique()
weird_data2 = weird_data[["id", "year", "CYRB_XRND", "age", column_to_examine]]




print(f"These are some of the weird values: \n {weird_data2}")

print(f"Here are the ages listed: {weird_data_ages}")

print(f"There are a total of {weird_data2.count()} weird values")

