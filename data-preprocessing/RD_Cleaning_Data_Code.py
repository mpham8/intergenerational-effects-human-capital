""" Bijan Taheri
Professor Stephen O'Connell / Michael Pham
June 2025
Pytohn 3.13


This code is meant to clean the data collected from the NLSY79 survey. It is meant to run in the base directory of the project. 

This code creates a child-by-period panel, where each data point represents a variable for a child in a specific period. Each period corresponds to an age range of the child. The periods are as follows: 
Period 0: age 0-5 (label: pre-elementary)
Period 1: age 6-9 (label: elementary)
Period 2: age 10-14 (label: secondary)
Period 3: age 15-19 (label: high school)

The code performs the following tasks:
0. Delete any existing output files to avoid confusion (make sure to respond to the input!)
1. Load the data (for the file path, leave it as a variable to be specified later)
2. Create a child by age panel, with the child ID and the age of the child with the CNLSY79 data
3. Combine this data with the data from the NLSY79 (which surveys the mother)
4. Create the child by period table by taking the middle value for each period (for example, age 12 for Period 2), and linearly interpolating between the different values in each period when applicable (for example, interpolating between math scores at ages 7 and 8 to find the best estimate for Period 1 (age 6-9). 
5. Save the cleaned data to a new .csv file

Dependencies:
- pandas
- numpy
- os
- wakepy (for keeping the script running)



"""

# LIBRARIES
import pandas as pd
import numpy as np
import os
from wakepy import keep




# CONSTANTS

# File paths

# Input files 
nls_file_path = 'data-preprocessing/06-04-12pm-renamed.csv'  # Update this path as needed
mother_data_file_path = 'data-preprocessing/06-04-mother-simple-renamed.csv'  # File containing mother data, update this path as needed

# Output files
nan_file_path = 'data-preprocessing/nan_columns_testing.csv'  # File to save columns with NaN values for further investigation
age_output_file_path = 'data-preprocessing/child_age_panel_testing.csv'
period_output_file_path = 'data-preprocessing/child_period_panel_testing.csv'  # File to save the child by period data
nls_file_path = 'data-preprocessing/06-04-11am-renamed.csv'  # Update this path as needed


# Age periods dictionary
age_periods = {
    0: (0, 5), # Pre-elementary
    1: (6, 9), # Elementary
    2: (10, 14), # Secondary
    3: (15, 19) # High school
}

# List of special columns that need to be handled separately
special_columns_excluding_dates = [
    'HGC_OF_MOTHER_AS_OF_MAY_1_R_', 
]


# List of prefixes for columns that should be removed when naming columns
column_prefixes_to_remove = [
    'HOME_A_0_2_', 
    'HOME_B_3_5_',
    'HOME_C_6_9_',
    'HOME_D_10_14_', 
    'HOME_B_3YRS_',
    'HOME_C_4_5_' 
    'HOME_C_6_',
    'HOME_D_10_',
    'HOME_A_', 
    'HOME_B_',
    'HOME_C_',
    'HOME_D_',
    'CHECK_'
]

# Dictionary to map poorly-named columns to their intended names (for CLNS79 data)
poorly_named_columns = {
    'TYPE_OF_SCHOOL_94_': 'TYPE_OF_SCHOOL_',
    'TYPE_OF_SCHOOL_96_': 'TYPE_OF_SCHOOL_',
    'SAMPLE_RACE_78SCRN': 'MOTHER_RACE_XRND',
    # TODO: Add any poorly named columns here, e.g. 'old_name': 'new_name'
    # This can be used for both renaming columns and for handling special cases
}





# FUNCTIONS

# Function to create the child by age panel from a dataframe

def create_child_by_age_panel(nls_data: pd.DataFrame) -> pd.DataFrame:
    """
    Creates a child-by-age panel from the CNLSY79 data.
    Parameters:
        nls_data (pd.DataFrame): The input DataFrame containing CNLSY79 data.
    Returns:
        pd.DataFrame: A DataFrame containing child-by-age data, with columns for child ID, age, and other variables.
    """
    # A row in our new dataframe might look like this:
    # | id | age | math_score | reading_score | ... |

    new_data = pd.DataFrame()
    # Create the 'id' column such that it contains 21 unique child IDs for every child in the NLSY79 data (one for each age from -1 to 19)
    new_data['id'] = np.repeat(nls_data['id'].unique(), 21)
    # Create the 'age' column such that it contains the ages from 0 to 19 for each child
    new_data['age'] = np.tile(np.arange(21)-1, len(nls_data['id'].unique()))

    print(new_data.head())
    for column in nls_data.columns:
        print(f"Processing column: {column}")
        # Skip the 'id' column
        if column == 'id':
            continue
        
        # If the column ends in "XRND," there is no specific date attached to it, and we can fill it in for all ages
        if column.endswith('XRND'):
            new_data[column] = np.repeat(nls_data[column].values, 21)  # Repeat the values for each age
            
                
        
        # If the column ends in a date (e.g. 1979, 1980, etc.), we need to find the age of the child at that date
        elif column[-4:].isdigit():  # Check if the last 4 characters are digits
            year = int(column[-4:])

            # Find the column name, removing the year part
            column_name = column[:-4]

            # Filter out unwanted prefixes from the column name
            for prefix in column_prefixes_to_remove:
                if column_name.startswith(prefix):
                    print(f"Removing prefix '{prefix}' from column name: {column_name}")
                    # Remove the prefix from the column name
                    column_name = column_name[len(prefix):]

            # If the column name is in the poorly named columns dictionary, rename it with the better name
            if column_name in poorly_named_columns:
                column_name = poorly_named_columns[column_name]
            

            # If the column name is in the special columns list, we will handle it separately
            if column_name in special_columns_excluding_dates:
                # print(f"Skipping special column: {column}")
                continue


            # # FOR TESTING PURPOSES: continue if not in the special columns for testing
            # if column_name not in special_columns_for_testing:
            #     # print(f"Skipping column {column} as it is not in the special columns for testing.")
            #     continue

            

            print(f"Processing column with year: {year}, base column name: {column_name}")
            
            # Calculate the age of the child at that year
            nls_data['age'] = year - nls_data['CYRB_XRND']
            
            

            # Check if the column already exists in new_data
            if column_name not in new_data.columns:
                # If not, create it with NaN values
                new_data[column_name] = np.nan
            
            
            # TODO: Since the IDs are unique and the ages are tiled, we can do this more efficiently
            for id in new_data['id']:

                # Get the value for this id and column
                value = nls_data[nls_data['id'] == id][column].values[0]
                age = nls_data[nls_data['id'] == id]['age'].values[0]
                # Fill in the value for the corresponding age
                new_data.loc[(new_data['id'] == id) & (new_data['age'] == age), column_name] = value

        
        
        
        
        
        # If neither of these are true, throw an error
        else: 
            raise ValueError(f"Column {column} does not end with 'XRND' or a year. Please check the data format.")

    return new_data




# Function to create the child by period table from the child by age panel
def create_period_data(df: pd.DataFrame, age_periods: dict) -> pd.DataFrame:
    """
    Creates a child-by-period table from a child-by-age panel.

    Parameters:
        df (pd.DataFrame): The input DataFrame containing child-by-age data.
        age_periods (dict): A dictionary mapping period numbers to age ranges (start_age, end_age).

    Returns:
        pd.DataFrame: A DataFrame containing interpolated values for each period, with columns for child ID, age, period, and other variables.
    """
    period_data = pd.DataFrame()
    for id in df['id'].unique():
        for period, (start_age, end_age) in age_periods.items():

            # Filter the data for the current period and child ID
            period_df = df[(df['age'] >= start_age) & (df['age'] <= end_age) & (df['id'] == id)].copy()

            # Calculate the middle age for the period
            middle_age = (start_age + end_age) / 2
            
            # Interpolate values for each column in the period
            interpolated_values = {}
            for column in df.columns:
                # Skip the 'id', 'age', and 'period' columns
                if column in ['id', 'age', 'period']:
                    continue

                # Interpolate values for the current column
                filtered_column = period_df[column][period_df[column] >= 0]  # Filter out negative values
                if filtered_column.isna().all():
                    ...
                # If the column has only one valid value, use that value for the period
                elif len(filtered_column) == 1:
                    # Use the single value for the middle age
                    interpolated_values[column] = filtered_column.iloc[0]
                else:
                    # Use np.interp to interpolate the values for the middle age
                    # Use np.interp to interpolate the values for the middle age

                    interpolated_values[column] = np.interp(middle_age, period_df['age'], period_df[column])

            # Ensure period_df is not empty before creating a new row
            if not period_df.empty:
                # Create a new row for the period data
                new_row = {'id': period_df['id'].iloc[0], 'period': period}
                new_row.update(interpolated_values)
                
                # Add the new row to the period_data DataFrame
                period_data = pd.concat([period_data, pd.DataFrame([new_row])], ignore_index=True)
            # If period_df is empty, we add an empty row with the id and period
            else:
                new_row = {'id': id, 'period': period}
                # Add the new row to the period_data DataFrame
                period_data = pd.concat([period_data, pd.DataFrame([new_row])], ignore_index=True)

    return period_data




# --------------------------------- MAIN SCRIPT -----------------------------------------
# =======================================================================================


# 0. Deleting any existing output files to avoid confusion
query = input("Would you like to delete any existing output files? (yes/no): ").strip().lower()
# List of output files to delete
output_files = [nan_file_path, age_output_file_path, period_output_file_path]
if query == 'yes':
    # Deleting the output files if they exist
    for file in output_files:
        if os.path.exists(file):
            os.remove(file)
            print(f"Deleted file: {file}")
        else:
            print(f"File not found, skipping deletion: {file}")
else:
    # Raising warnings if the files exist but aren't deleted
    print("Skipping deletion of existing output files.")
    for file in output_files:
        if os.path.exists(file):
            print(f"Warning: File {file} already exists. This may cause errors in the code.")



# 1. Loading the data
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

# FOR TESTING PURPOSES: shorten the data to only include a few rows

nls_data = nls_data.head(50)  # Uncomment this line to limit the data for testing purposes



# 2. Create the child by age panel
with keep.running(): # Keep the script running to avoid premature termination

    # Creating the basic panel, excluding special columns
    new_data = create_child_by_age_panel(nls_data)

    
    # Handling the special columns (TODO)

    # HGC_OF_MOTHER_AS_OF_MAY_1_R_ is a special column, for which we want to only keep the value of the pre-birth age of the child (age -1)
    # if 'HGC_OF_MOTHER_AS_OF_MAY_1_R_' in new_data.columns:
    #     # Create a new column for the mother's education level at birth
    #     new_data['mother_education_at_birth'] = np.nan
        
    #     # Fill in the mother's education level at birth for each child
    #     for id in new_data['id'].unique():
    #         # Get the value for this id and column
    #         if (new_data[(new_data['id'] == id) & (new_data['age'] == -1)].empty):
    #             # If there is no value for age -1, we will fill it in with -10 (maybe we consider filing in with age = 0?)
    #             value = -10 
    #         else:
    #             value = new_data[(new_data['id'] == id) & (new_data['age'] == -1)]['HGC_OF_MOTHER_AS_OF_MAY_1_R_'].values[0]
    #         # Fill in the value for the corresponding age -1
    #         new_data.loc[(new_data['id'] == id), 'mother_education_at_birth'] = value
    # else:
    #     raise ValueError("Column 'HGC_OF_MOTHER_AS_OF_MAY_1_R_' not found in the data. Check the data.")


    # TODO: Combine the data with the regular NLSY79 data
    try:
        mother_data = pd.read_csv(mother_data_file_path)
    except FileNotFoundError:
        print(f"Error: The file {nls_file_path} was not found. Please check the file path.")
        raise
    
    mother_data = pd.DataFrame(mother_data)
    
    for column in mother_data.columns:

        # Skip the 'id' column
        if column == 'CASEID_1979':
            continue
            
                
        
        # If the column ends in a date (e.g. 1979, 1980, etc.), we need to find the age of the child at that date
        elif column[-4:].isdigit():  # Check if the last 4 characters are digits
            year = int(column[-4:])

            # Find the column name, removing the year part
            column_name = column[:-4]

            # Filter out unwanted prefixes from the column name
            for prefix in column_prefixes_to_remove:
                if column_name.startswith(prefix):
                    print(f"Removing prefix '{prefix}' from column name: {column_name}")
                    # Remove the prefix from the column name
                    column_name = column_name[len(prefix):]

            # If the column name is in the poorly named columns dictionary, rename it with the better name
            if column_name in poorly_named_columns:
                column_name = poorly_named_columns[column_name]
            

            # If the column name is in the special columns list, we will handle it separately
            if column_name in special_columns_excluding_dates:
                # print(f"Skipping special column: {column}")
                continue


            # # FOR TESTING PURPOSES: continue if not in the special columns for testing
            # if column_name not in special_columns_for_testing:
            #     # print(f"Skipping column {column} as it is not in the special columns for testing.")
            #     continue

            

            print(f"Processing column with year: {year}, base column name: {column_name}")
            
            
            
            

            # Check if the column already exists in new_data
            if column_name not in new_data.columns:
                # If not, create it with NaN values
                new_data[column_name] = np.nan
            
            
            # TODO: Since the IDs are unique and the ages are tiled, we can do this more efficiently
            for id in nls_data['id']:

                # Get the relevant data
                nls_data_id = nls_data[nls_data['id'] == id]
                # Get the age of the child at that year
                child_age = year - nls_data_id['CYRB_XRND'].values[0]
                print(f"CNLSY79 mother ID: {nls_data_id['MPUBID_XRND']}")
                # Get the value for this id and column from the mother data
                value = mother_data[mother_data['CASEID_1979'] == nls_data_id['MPUBID_XRND'].values[0]][column].values[0]


                # Fill in the value for the corresponding age
                new_data.loc[(new_data['id'] == id) & (new_data['age'] == child_age), column_name] = value


            # Ensure we have a pre-birth age (-1) value for each child for the column
            if new_data[(new_data['age'] == -1) & (new_data[column_name].notna())].empty:
                # If there is no value for age -1, we will fill it in with age -2, or -3, and so on
                # Get the greatest negative age that has a value for the column (i.e., the most recent age before birth)
                negative_ages = new_data[(new_data['age'] < 0) & (new_data[column_name].notna())]['age']
                if not negative_ages.empty:
                    greatest_negative_age = negative_ages.max()
                    # Fill in the value for the corresponding age -1
                    new_data.loc[(new_data['id'] == id) & (new_data['age'] == -1), column_name] = new_data.loc[(new_data['id'] == id) & (new_data['age'] == greatest_negative_age), column_name].values[0]
                else: 
                    print(f"Warning: No negative ages found for id {id} in column {column_name}. Unable to fill in pre-birth age (-1) value.")

        # If the column ends with XRND (or anything else, for that matter), it doesn't need to be adjusted for age, so we can fill it in for all ages, merging it with the new_data DataFrame
        else: 
            # Put out a warning if column doesn't end with XRND
            if not column.endswith("XRND"):
                print(f"Column {column} does not end with 'XRND'. It will be added to all ages for each child.")
            # Add the mother's XRND variable to all ages for each child
            # First, map CASEID_1979 to id
            mother_id = mother_data['CASEID_1979']
            mother_col = mother_data[column]
            # Create a mapping from CASEID_1979 to the column value
            mother_map = dict(zip(mother_id, mother_col))
            # Fill in the value for all ages for each child
            new_data[column] = new_data['MPUBID_XRND'].map(mother_map)



    # Filter out rows where age > 19 and age < -1 (age -1 is the pre-birth age)
    new_data = new_data[(new_data['age'] >= -1) & (new_data['age'] <= 19)]


    # 2a. Running some checks on the new_data DataFrame and saving it to a new file

    # Check for nan values in the new_data DataFrame. If such values exist, print the number of nan values and the columns they are in
    nan_counts = new_data.isna().sum()
    # Identify columns where all values are NaN
    all_nan_columns = nan_counts[nan_counts == len(new_data)].index.tolist()
    if all_nan_columns:
        print("Warning: The following columns have all NaN values in the new_data DataFrame:")
        print(all_nan_columns)

        # Save only the columns with all NaN values to a separate file for further investigation
        nan_data = new_data[all_nan_columns]
        nan_data.to_csv(nan_file_path, index=False)
        print(f"All-NaN columns data saved to {nan_file_path}")

    else:
        print("No columns with all NaN values found in the new_data DataFrame.")

    # Save the new_data DataFrame to a new file
    new_data.to_csv(age_output_file_path)
    print(f"Cleaned data saved to {age_output_file_path}")





# 3. Create the child by period table


# Create the period data
period_data = create_period_data(new_data, age_periods)


# Print the first few rows of the period data to verify
print("Created period data. Here are the first few rows:")
print(period_data.head())

# Provide a summary of the period data
print("\nSummary of period data:")
print(period_data.describe(include='all'))

# Print all the columns in the period data
print("\nColumns in the period data:")
print(period_data.columns.tolist())


# 4. Save the period data to a new file

period_data.to_csv(period_output_file_path)
print(f"Period data saved to {period_output_file_path}")
# End of the script