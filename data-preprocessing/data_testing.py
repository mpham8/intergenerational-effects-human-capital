import numpy as np
import pandas as pd
from RD_Cleaning_Data_Code import nls_file_path, age_periods
import matplotlib.pyplot as plt
from important_dictionary_variables import categories_of_variables
import seaborn as sns

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



age_data = pd.read_csv("data-preprocessing/Processed_Data/child_age_panel_no_inflation_adjustment.csv")
period_data = pd.read_csv("data-preprocessing/Processed_Data/child_period_panel_BEST.csv")
period_data_no_filled_edges = pd.read_csv("data-preprocessing/Processed_Data/child_period_panel_no_filled_edges.csv")





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





def plot_category_availability(period_data, category_columns):
    """
    Plots bar charts for test score availability per child in each period.
    1. Histogram: Percent of children with X test scores available in each period.
    2. Bar chart: Percent of children with each test score in each period.
    """
    periods = period_data['period'].unique()
    print(periods)
    for period in periods:
        data_period = period_data[period_data['period'] == period]
        print(data_period[[col for col in period_data.columns if col in ["id", "period"] or col in category_columns]].head())
        unique_ids = data_period['id'].unique()
        total_children = len(unique_ids)

        # 1. Histogram: Percent of children with X test scores available
        scores_per_child = data_period.groupby('id')[category_columns].apply(lambda df: df.notnull().sum())
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
        for col in category_columns:
            ids_with_score = data_period.loc[data_period[col].notnull(), 'id'].unique()
            percent = len(ids_with_score) / total_children
            percent_per_score.append(percent)
        plt.figure(figsize=(10, 4))
        plt.bar(category_columns, percent_per_score, edgecolor='black')
        plt.title(f'Percent of Children with Each Test Score in Period {period}')
        plt.xlabel('Test Score')
        plt.ylabel('Percent of Children')
        plt.ylim([0, 2])
        plt.tight_layout()
        plt.savefig(data_testing_folder + f"Period_{period}_categorical_test_counts.png")
        plt.clf()
        plt.cla()
        plt.close()

def plot_variable_availability(data, column): 
    """
    Plots the availability of a variable across periods.
    """
    if 'period' in data.columns: 
        time_identifier = 'period'
    elif 'age' in data.columns: 
        time_identifier = 'age'
    else: 
        print("No time identifying column found")
        return
    times = data[time_identifier].unique()
    
    # Plotting a bar chart of the percent of children with the variable available by period
    percent_per_time = []
    for time in times:
        data_time = data[data[time_identifier] == time]
        total_children = len(data_time['id'].unique())
        count_with_variable = data_time[column].notnull().sum()
        percent = count_with_variable / total_children
        percent_per_time.append(percent)
    plt.figure(figsize=(10, 4))
    plt.bar(times, percent_per_time, edgecolor='black')
    plt.title(f'Percent of Children with {column} Available by {time_identifier}')
    plt.xlabel('Period')
    plt.ylabel('Percent of Children')
    plt.ylim([0, 1])
    plt.tight_layout()
    plt.show()
    plt.clf()
    plt.cla()
    plt.close()
    
def get_unique_ids(age_data, cols: list): 
    total_ids = age_data['id'].nunique()
    for col in cols:
        if col in age_data.columns:
            # count the number of unique id's with at least one value for an age
            unique_ids = age_data.loc[age_data[col] >= 0, 'id'].unique()
            print(f"Column '{col}' has {len(unique_ids)} unique ids with responses, which is {len(unique_ids) / total_ids:.2%} of the total ids.")

        else:
            print(f"Column '{col}' is not present in the period data.")


def plot_variable_histogram_over_time(data, column, times=None):
    """
    Plots a histogram of the variable over time.
    """
    if 'period' in data.columns:
        time_identifier = 'period'
    elif 'age' in data.columns:
        time_identifier = 'age'
    else:
        print("No time identifying column found")
        return
    if times is None:
        times = data[time_identifier].unique()
    
    plt.figure(figsize=(10, 4))
    for time in times:
        data_time = data[data[time_identifier] == time]
        plt.hist(data_time[column].dropna(), alpha=0.5, label=f'{time_identifier} {time}', bins=30)
    
    plt.title(f'Histogram of {column} Over Time')
    plt.xlabel(column)
    plt.ylabel('Frequency')
    plt.legend()
    plt.tight_layout()
    plt.show()
    plt.clf()
    plt.cla()
    plt.close()


def plot_variable_over_time(data, column, time_col='period'):
    # Filter to only rows where both period and variable are not null
    data = data[[time_col, column]].dropna()

    if time_col not in data.columns or column not in data.columns:
        print(f"Column '{time_col}' or '{column}' is not present in the data.")
        return

    plt.figure(figsize=(12, 6))
    sns.boxplot(x=time_col, y=column, data=data)
    plt.title(f'Distribution of {column} over time')
    plt.xlabel('Period')
    plt.ylabel(column)
    plt.grid(True)
    plt.tight_layout()
    plt.show()
# plot_category_availability(period_data, categories_of_variables["Child_Human_Capital"])
# plot_variable_availability(period_data, "PIAT_MATH")
# get_unique_ids(age_data, "PIAT_MATH")
# plt.hist(period_data["HOW_MANY_BOOKS"])
# plot_variable_availability(age_data, "PIAT_MATH")
# plot_variable_histogram_over_time(age_data[age_data >= 0], "PIAT_MATH", times=[13, 14, 15, 16, 17, 18, 19])
# plot_variable_availability(age_data[age_data >= 0], "PIAT_MATH")


# TODO: find out where all the period 3 test scores are coming from
# i.e., create a bar chart showing what age the test scores originate from that we are using for "period 3" 
# Also output how many of them are from the actual age (age 19)
def plot_test_score_max_period_origin(): 
    for test_score in categories_of_variables["Child_Human_Capital"]:
        plotting_dict = {
            13: 0, 
            14: 0,
            15: 0,
            16: 0,
            17: 0,
            18: 0,
            19: 0
        }
        for child_id in age_data['id'].unique(): 
            # Get the age of the child for the most recent available test score (value > 0)
            child_age = age_data.loc[(age_data['id'] == child_id) & (age_data[test_score] > 0), 'age'].max()
            if child_age in plotting_dict:
                plotting_dict[child_age] += 1
        
        # Create a bar chart showing the number of test scores by age
        plt.figure(figsize=(10, 4))
        plt.bar(plotting_dict.keys(), plotting_dict.values(), edgecolor='black')
        plt.title(f'{test_score} Score At Maximum Age')
        plt.xlabel('Age')
        plt.ylabel(f'Number of {test_score} Scores')
        plt.savefig(data_testing_folder + f"{test_score}_scores_at_max_age.png")
        plt.clf()
        plt.cla()
        plt.close()

# TODO: write a general function to plot the average value of a variable over time with boxplots (or something else showing std's, like the climate change projections)

# TODO: plot number of data points by period (total, aggregated over all columns and all children)
def plot_data_points_by_period(data, time_col='period'):
    """
    Plots the number of data points by period.
    """
    if time_col not in data.columns:
        print(f"Column '{time_col}' is not present in the data.")
        return
    
    # Count the number of values that are positive (i.e., not negative or NaN) for each period
    time_counts = []
    time_values = data[time_col].unique()
    for time in time_values: 
        time_count = 0
        for col in data.columns: 
            time_count += data.loc[data[col].notna() & (data[col] >= 0) & (data[time_col] == time), col].count()
        time_counts.append(time_count)
   
    # TODO: try this out with no filled edge data
    print(time_counts)
    plt.figure(figsize=(10, 4))
    plt.bar(time_values, time_counts, width=1, bottom=0)
    plt.title(f'Number of Data Points by {time_col}')
    plt.xlabel(time_col)
    plt.ylabel('Number of Data Points')
    plt.tight_layout()
    plt.show()
# 
# plot_data_points_by_period(period_data_no_filled_edges, time_col='period')
# plot_variable_over_time(age_data[age_data > 0], "PIAT_MATH", time_col='age')
# plot_variable_over_time(age_data[age_data > 0], "PPVT", time_col='age')
# plt.hist(period_data.loc[period_data["TRANSFER_INCOME"] > 0, "TRANSFER_INCOME"])
# plt.show()

# sns.displot(period_data.loc[period_data["TRANSFER_INCOME"] > 0, "TRANSFER_INCOME"])
# plt.show()

print(age_data.loc[(age_data["SSI_TOTAL"] > 30000) & (age_data["year"] == 2014), ['id', 'age', 'year', 'MPUBID_XRND', 'SSI_TOTAL', 'WELFARE_AMT', 'TRANSFER_INCOME']].sort_values('year'))
print(age_data.loc[(age_data["AFDC_TOTAL"] > 75000) & (age_data["year"] == 1988), ['id', 'age', 'year', 'MPUBID_XRND', 'AFDC_TOTAL', 'WELFARE_AMT', 'TRANSFER_INCOME']].sort_values('year'))
print(age_data.loc[(age_data["TRANSFER_INCOME"] > 200000), ['id', 'age', 'year', 'MPUBID_XRND', 'SSI_TOTAL', 'AFDC_TOTAL', 'FDSTMPS_TOTAL', 'WELFARE_AMT', 'UNEMPR_TOTAL', 'UNEMPSP_TOTAL', 'TRANSFER_INCOME']].sort_values('year'))

print(age_data.loc[(age_data['SSI_TOTAL'] < age_data['SSDI_TOTAL']) & (age_data['SSI_TOTAL'] > 0), ['id', 'age', 'year', 'MPUBID_XRND', 'SSI_TOTAL', 'SSDI_TOTAL', 'WELFARE_AMT', 'TRANSFER_INCOME']])
print(age_data.loc[(age_data["SSI_TOTAL"] > 75000) & (age_data["year"] == 2014), 'MPUBID_XRND'].unique())
print(age_data.loc[(age_data["AFDC_TOTAL"] > 75000) & (age_data["year"] == 1988), 'MPUBID_XRND'].unique())
# TODO: figure out number of siblings that may be missing from the data
num_missing_siblings = 0
missing_siblings = [] # List to store IDs of children with missing siblings
for child_id in nls_data['id'].unique(): 
    mother_id = nls_data.loc[nls_data['id'] == child_id, 'MPUBID_XRND'].values[0]
    siblings = nls_data.loc[nls_data['MPUBID_XRND'] == mother_id, 'id'].unique()
    # Test if any siblings are missing by ensuring that all id's are one apart (e.g. 801, 802, 803)
    if len(siblings) > 1:
        siblings = np.sort(siblings)
        # Check if siblings are consecutive. Count how many are missing
        for i in range(len(siblings) - 1):
            if siblings[i + 1] - siblings[i] > 1:
                num_missing_siblings += (siblings[i + 1] - siblings[i]) - 1
                missing_siblings.extend(range(siblings[i] + 1, siblings[i + 1]))
# Answer: there are very little missing siblings! In fact, only 2 children have a missing sibling

print(f"Number of missing siblings: {num_missing_siblings}")
print(f"Missing siblings IDs: {missing_siblings}")
