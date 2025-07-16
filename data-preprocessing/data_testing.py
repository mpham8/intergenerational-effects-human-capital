import numpy as np
import pandas as pd
from RD_Cleaning_Data_Code import nls_file_path, age_periods
import matplotlib.pyplot as plt
from important_dictionary_variables import categories_of_variables

data_testing_folder = "data-preprocessing/Initial_Preprocessing/data-testing/"
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



age_data = pd.read_csv("data-preprocessing/Processed_Data/child_age_panel_BEST.csv")
period_data = pd.read_csv("data-preprocessing/Processed_Data/child_period_panel_BEST.csv")

# Seeing how many people answer the "headstart questions"
for column in ["CHILD_EVER_ENROLLED_IN_HEAD", "HOW_LONG_CHILD_WAS_IN_HEAD", "CHILD_AGE_WHEN_1ST_ATTD_HEA"]: 
    count = age_data.count()[column]
    total = age_data.count()["id"]
    print(f"Column {column} has count {count}, being answered for {count/total}")



# # Examining weird data anomalies (age)
# column_to_examine = "MOM_HELPS_CH_LEARN_NUMBERS"
# age_to_examine = 3
# keep_columns = ["id", "year", "CYRB_XRND", column_to_examine]
# age_dist = age_data.loc[(age_data[column_to_examine] > 0), "age"]
# plt.hist(age_dist, bins=np.arange(15)+0.5)
# plt.show()

# weird_data = age_data.loc[(age_data["age"] < age_to_examine) & (age_data[column_to_examine] > 0)]
# weird_data_ages = weird_data["age"].unique()
# weird_data2 = weird_data[["id", "year", "CYRB_XRND", "age", column_to_examine]].sort_values("year")

# weird_data_ids = weird_data2.loc[weird_data2["year"] == 1994, "id"].values


# print(f"These are some of the weird values: \n {weird_data2}")

# print(f"Here are the ages listed: {weird_data_ages}")

# print(f"There are a total of {weird_data2.count()} weird values")

# print(f"Here are the weird data IDs you requested: \n{weird_data_ids}")


# This is a temporary check to see how many people responded to the Government_Input questions
print("Checking Government_Input columns...")
total_ids = age_data['id'].nunique()
for col in categories_of_variables["Government_Inputs"]:
    if col in age_data.columns:
        # count the number of unique id's with at least one value for an age
        unique_ids = age_data.loc[age_data[col] >= 0, 'id'].unique()
        print(f"Column '{col}' has {len(unique_ids)} unique ids with responses, which is {len(unique_ids) / total_ids:.2%} of the total ids.")

    else:
        print(f"Column '{col}' is not present in the period data.")

def plot_test_score_availability(period_data, test_score_columns):
    """
    Plots bar charts for test score availability per child in each period.
    1. Histogram: Percent of children with X test scores available in each period.
    2. Bar chart: Percent of children with each test score in each period.
    """
    periods = period_data['period'].unique()
    print(periods)
    for period in periods:
        data_period = period_data[period_data['period'] == period]
        print(data_period[[col for col in period_data.columns if col in ["id", "period"] or col in test_score_columns]].head())
        unique_ids = data_period['id'].unique()
        total_children = len(unique_ids)

        # 1. Histogram: Percent of children with X test scores available
        scores_per_child = data_period.groupby('id')[test_score_columns].apply(lambda df: df.notnull().sum())
        print(scores_per_child.head())
        scores_per_child = scores_per_child.sum(axis=1)
        counts = scores_per_child.value_counts().sort_index()
        percents = counts / total_children
        plt.figure(figsize=(8, 4))
        plt.bar(percents.index, percents.values, edgecolor='black')
        plt.title(f'Percent of Children with X Test Scores in Period {period}')
        plt.xlabel('Number of Test Scores')
        plt.ylabel('Percent of Children')
        plt.ylim([0, 2])
        plt.tight_layout()
        plt.savefig(data_testing_folder + f"Period_{period}_num_test_counts.png")
        plt.clf()
        plt.cla()
        plt.close()

        # 2. Bar chart: Percent of children with each test score
        percent_per_score = []
        for col in test_score_columns:
            ids_with_score = data_period.loc[data_period[col].notnull(), 'id'].unique()
            percent = len(ids_with_score) / total_children
            percent_per_score.append(percent)
        plt.figure(figsize=(10, 4))
        plt.bar(test_score_columns, percent_per_score, edgecolor='black')
        plt.title(f'Percent of Children with Each Test Score in Period {period}')
        plt.xlabel('Test Score')
        plt.ylabel('Percent of Children')
        plt.ylim([0, 2])
        plt.tight_layout()
        plt.savefig(data_testing_folder + f"Period_{period}_categorical_test_counts.png")
        plt.clf()
        plt.cla()
        plt.close()

def plot_variable(period_data, column): 
    """
    Plots the availability of a variable across periods.
    """
    periods = period_data['period'].unique()
    
    # Plotting a bar chart of the percent of children with the variable available by period
    percent_per_period = []
    for period in periods:
        data_period = period_data[period_data['period'] == period]
        total_children = len(data_period['id'].unique())
        count_with_variable = data_period[column].notnull().sum()
        percent = count_with_variable / total_children
        percent_per_period.append(percent)
    plt.figure(figsize=(10, 4))
    plt.bar(periods, percent_per_period, edgecolor='black')
    plt.title(f'Percent of Children with {column} Available by Period')
    plt.xlabel('Period')
    plt.ylabel('Percent of Children')
    plt.ylim([0, 2])
    plt.tight_layout()
    plt.show()
    plt.clf()
    plt.cla()
    plt.close()
    
def get_unique_ids(age_data, col): 
    if col in age_data.columns:
        # count the number of unique id's with at least one value for an age
        unique_ids = age_data.loc[age_data[col] >= 0, 'id'].unique()
        print(f"Column '{col}' has {len(unique_ids)} unique ids with responses, which is {len(unique_ids) / total_ids:.2%} of the total ids.")

    else:
        print(f"Column '{col}' is not present in the period data.")
# plot_test_score_availability(period_data, categories_of_variables["Child_Human_Capital"])
plot_variable(period_data, "PIAT_MATH")
get_unique_ids(age_data, "PIAT_MATH")
plt.hist(period_data["HOW_MANY_BOOKS"])
# for i in range(13): 
#     plot_variable(period_data, f"ASVAB_{i+1}")