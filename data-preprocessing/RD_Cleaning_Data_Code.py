""" Bijan Taheri
Professor Stephen O'Connell / Michael Pham
June 2025
Pytohn 3.13


This code is meant to clean the data collected from the NLSY79 survey. It is meant to run in the base directory of the project. 

This code creates a child-by-period panel, where each data point represents a variable for a child in a specific period. Each period corresponds to an age range of the child. The periods are as follows: 
Period -1: age -1 (label: pre-birth)
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
- scipy (for pandas under-the-hood interpolation)



"""

# LIBRARIES
import pandas as pd
import numpy as np
import os
from wakepy import keep




# CONSTANTS

# File paths

# Input files 
nls_file_path = 'data-preprocessing/06-05-7pm-renamed.csv'  # Update this path as needed
mother_data_file_path = 'data-preprocessing/06-05-mother-simple-renamed.csv'  # File containing mother data, update this path as needed

# Output files
nan_file_path = 'data-preprocessing/nan_columns_testing.csv'  # File to save columns with NaN values for further investigation
age_output_file_path = 'data-preprocessing/child_age_panel_testing.csv'
period_output_file_path = 'data-preprocessing/child_period_panel_testing.csv'  # File to save the child by period data

# Defining terms for rescaling data
SEVERAL_TIMES_PER_YEAR = 10
SEVERAL_TIMES_PER_MONTH = 7
SEVERAL_TIMES_PER_WEEK = 4
MORE_THAN_ONCE_PER_DAY = 2
WEEKS_PER_MONTH = 4.345


# Age periods dictionary
age_periods = {
    -1: (-1, -1), # Pre-birth
    0: (0, 5), # Pre-elementary
    1: (6, 9), # Elementary
    2: (10, 14), # Secondary
    3: (15, 19) # High school
}

# List of special columns that need to be handled separately
special_columns_excluding_dates = [
    'HGC_OF_MOTHER_AS_OF_MAY_1_R', # NOTE: for now, this isn't handled. I'm pretty sure 
]




# List of prefixes for columns that should be removed when naming columns
column_prefixes_to_remove = [
    'HOME_A_0_2_', 
    'HOME_B_3_5_',
    'HOME_C_6_9_',
    'HOME_D_10_14_', 
    'HOME_B_3YRS_',
    'HOME_B_4_5_',
    'HOME_C_4_5_',
    'HOME_C_6_',
    'HOME_D_10_',
    'HOME_A_', 
    'HOME_B_',
    'HOME_C_',
    'HOME_D_',
    'CHECK_'
]

# Dictionary to map poorly-named columns to their intended names (basically, handling exceptions in the CLNS79 data)
poorly_named_columns = {
    'MOM_HELPS_CH_LE': 'MOM_HELPS_CH_LEARN_NUMBERS',
    'MOM_HELPS_CH_LEA': 'MOM_HELPS_CH_LEARN_NUMBERS',
    'MOM_HELPS_CH_LEARN_N': 'MOM_HELPS_CH_LEARN_NUMBERS',
    'MOM_HELPS_CH_LEARN_A': 'MOM_HELPS_CH_LEARN_ALPHABET',
    'MOM_HELPS_CH_LEARN_ALPHABE' : 'MOM_HELPS_CH_LEARN_ALPHABET',
    'MOM_HELPS_CH_LEARN_C': 'MOM_HELPS_CH_LEARN_COLORS',
    'MOM_HELPS_CH_LEARN_S': 'MOM_HELPS_CH_LEARN_SHAPES',
    'MOM_HELPS_CH_W_N': 'MOM_HELPS_CH_W_NONE',
    'TYPE_OF_SCHOOL_94': 'TYPE_OF_SCHOOL',
    'TYPE_OF_SCHOOL_96': 'TYPE_OF_SCHOOL',
    # NOTE: confused, what's the difference between "C" and "Y"
    # 'MS_TYPE_OF_SCHOOL_CHILD_ATT' : 'TYPE_OF_SCHOOL',
    # 'MS_TYPE_OF_SCH_CHD_ATTNDS_C' : 'TYPE_OF_SCHOOL',
    # 'TYPE_OF_SCHOOL_CHILD_ATTEND' : 'TYPE_OF_SCHOOL',
    # 'SCHOOL_CHILD_ATTENDS': 'TYPE_OF_SCHOOL',
    # 'TYPE_SCHOOL_CHILD_ATTENDS_V' : 'SCHOOL_CHILD_ATTENDS_RECD',
    'CHILD_S_AGE_WHEN_1ST_ATTD_H': 'CHILD_AGE_WHEN_1ST_ATTD_HEA',
    'HOW_OFT_CH_EATS' : 'HOW_OFT_CH_EATS_W', 
    'HOW_OFT_CH_EAT' : 'HOW_OFT_CH_EATS_W',
    'HOW_OFT_CH_EATS_W_MO' : 'HOW_OFT_CH_EATS_W',
    'HOW_OFTEN_MOM_R' : 'HOW_OFTEN_MOM_READS',
    'HOW_OFTEN_MOM_RE' : 'HOW_OFTEN_MOM_READS',
    'HOW_OFTEN_MOM_READ' : 'HOW_OFTEN_MOM_READS',
    'HOW_OFT_MOM_READ_TO' : 'HOW_OFTEN_MOM_READS',
    'HOW_OFT_DOES_MOM_REA' : 'HOW_OFTEN_MOM_READS',
    'HOW_OFT_CH_W_D' : 'HOW_OFT_CH_W_DAD',
    'HOW_OFT_CH_W_DAD_O' : 'HOW_OFT_CH_W_DAD',
    'HOW_OFT_CH_TAK' : 'HOW_OFT_CH_TAKEN',
    'HOW_OFT_CH_TAKE' : 'HOW_OFT_CH_TAKEN',
    'HOW_OFT_CH_TAKEN_T' : 'HOW_OFT_CH_TAKEN',
    'HOW_OFT_CH_TAKEN_TO' : 'HOW_OFT_CH_TAKEN',
    'HOW_OFT_TAKEN_TO_P' : 'HOW_OFT_TAKEN',
    'HOW_OFT_TAKEN_TO' : 'HOW_OFT_TAKEN',
    'HOW_OFT_TAKEN_T' : 'HOW_OFT_TAKEN',
    'MUSIC_INSTMT_CH_CA' : 'MUSIC_INSTMT_CH',
    'MUSIC_INSTMT_C' : 'MUSIC_INSTMT_CH',
    'IS_THERE_MUSIC_INSTR' : 'MUSIC_INSTMT_CH',
    'IF_LOW_GRADES_HE' : 'IF_LOW_GRADES',
    'IF_LOW_GRADES_HEL' : 'IF_LOW_GRADES', 
    'DO_PARS_DISCUSS' : 'DO_PARS_DISCUSS_TV',
    'DO_PARS_DISCUSS_T' : 'DO_PARS_DISCUSS_TV',
    'PARS_DISCUSS_TV_PROG' : 'DO_PARS_DISCUSS_TV',
    'PARS_DISCUSS_TV_PRGM' : 'DO_PARS_DISCUSS_TV',
    'HOW_MANY_BOOKS_C' : 'HOW_MANY_BOOKS',
    'HOW_MANY_BOOKS_CH' : 'HOW_MANY_BOOKS',
    'HOW_MANY_BOOKS_CHI' : 'HOW_MANY_BOOKS',
    'HOW_MANY_BOOKS_CHILD' : 'HOW_MANY_BOOKS',
    'HOW_MANY_BOOKS_DOES' : 'HOW_MANY_BOOKS',
    'HOW_OFT_CH_SPE' : 'HOW_OFT_CH_SPEND', 
    'HOW_OFT_CH_SPEND_T' : 'HOW_OFT_CH_SPEND',
    'HOW_OFT_CH_SPEND_TIM' : 'HOW_OFT_CH_SPEND',
    'CHILD_GET_SPEC_LE' : 'CHILD_GET_SPEC_LESSON',
    'CHILD_GET_SPEC_LESS' : 'CHILD_GET_SPEC_LESSON',
    'CHILD_GET_SPEC_LESSO' : 'CHILD_GET_SPEC_LESSON',
    'TOTAL_FAMILY_INCOME_FROM_AL' : 'TOTAL_FAMILY_INCOME_FR_ALL',
    # TODO: type school child attends
}

# TODO: create list to rename columns
better_named_columns = {
    'HOW_OFT_CH_TAKEN' : 'HOW_OFT_CH_TAKEN_TO_MUSEUM', 
    'HOW_OFT_TAKEN' : 'HOW_OFT_CH_TAKEN_TO_PERFORMANCE',
    'HOW_OFT_CH_SPEND' : 'HOW_OFT_CH_SPEND_TIME_W_DAD',
    'HOW_OFT_CH_W_DAD' : 'HOW_OFT_CH_W_DAD_OUTDOORS',
    'SAMPLE_RACE_78SCRN' : 'MOTHER_RACE_ENCODED',
    'SCHOOL_CHILD_ATTENDS_RECD' : 'TYPE_OF_SCHOOL_RECODE'
}

# TODO: rescaling variables 
# If over time, all variables will be rescaled to per year (easiest to do)
rescaling_variables = {
    'HOW_OFTEN_MOM_READS' : [0, SEVERAL_TIMES_PER_YEAR/52, SEVERAL_TIMES_PER_MONTH/WEEKS_PER_MONTH, 1, 3, 7], 
    'HOW_OFT_CH_EATS_W' : [MORE_THAN_ONCE_PER_DAY*7, 7, SEVERAL_TIMES_PER_WEEK, 1, 1/WEEKS_PER_MONTH, 0], 
    'HOW_OFT_CH_TAKEN_TO_MUSEUM' : [0, 1.5/52, SEVERAL_TIMES_PER_YEAR/52, 1/WEEKS_PER_MONTH, 1],
    'HOW_OFT_CH_TAKEN_TO_PERFORMANCE' : [0, 1.5/52, SEVERAL_TIMES_PER_YEAR/52, 1/WEEKS_PER_MONTH, 1],
    'HOW_OFT_CH_W_DAD' : [7, 4, 1, 1/WEEKS_PER_MONTH, 3/52, np.nan], 
    'HOW_OFT_CH_W_DAD_OUTDOORS' : [7, 4, 1, 1/WEEKS_PER_MONTH, 3/52, np.nan],
    'DO_PARS_DISCUSS_TV' : [0, 1, np.nan]
    
}

# List of variables to rescale that are edge cases (the variable scales differ with age)
# Each entry is [variable name, age range, set of new values]
rescaling_variables_by_age = [
    ('HOW_MANY_BOOKS', (0, 9), [0, 1.5, 6, 10]),
    ('HOW_MANY_BOOKS', (10, 14), [0, 5, 15, 20])
]




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
        # print(f"Processing column: {column}")
        # Skip the 'id' column
        if column == 'id':
            continue
        
        # If the column ends in "XRND," there is no specific date attached to it, and we can fill it in for all ages
        if column.endswith('XRND'):
            new_data[column] = np.repeat(nls_data[column].values, 21)  # Repeat the values for each age
            
                
        
        # If the column ends in a date (e.g. 1979, 1980, etc.), we need to find the age of the child at that date
        elif column[-4:].isdigit():  # Check if the last 4 characters are digits
            year = int(column[-4:])

            # Find the column name, removing the year part (plus the underscore)
            column_name = column[:-5]

            # Filter out unwanted prefixes from the column name
            for prefix in column_prefixes_to_remove:
                if column_name.startswith(prefix):
                    # print(f"Removing prefix '{prefix}' from column name: {column_name}")
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



            if column_name[-1].isdigit():
                # If the column name ends with a digit, remove the last two characters (the year and the underscore)
                column_name = column_name[:-2]
            
            # Removing leading and trailing underscores
            if column_name[-1] == "_":
                column_name = column_name[:-1]
            if column_name[0] == "_":
                column_name = column_name[1:]
        

            # Check if the column name starts or ends with a digit
            if column_name[0].isdigit() or column_name[-1].isdigit():
                print(f"Warning: Column name {column_name} starts or ends with a digit.")
                print(f"Column name before processing: {column}")
            
            # print(f"Processing column with year: {year}, base column name: {column_name}")
            # Calculate the age of the child at that year
            nls_data['age'] = year - nls_data['CYRB_XRND']
            
            

            # Check if the column already exists in new_data
            if column_name not in new_data.columns:
                # If not, create it with NaN values
                new_data[column_name] = np.nan
            
            
            # Efficiently assign values using vectorized operations
            # For each row in nls_data, set the value for the corresponding (id, age) in new_data
            id_values = nls_data['id'].values
            age_values = nls_data['age'].values if 'age' in nls_data.columns else np.full(len(nls_data), np.nan)
            col_values = nls_data[column].values

            # Create a DataFrame for merging
            temp_df = pd.DataFrame({
                'id': id_values,
                'age': age_values,
                column_name: col_values
            })

            # Merge on id and age, updating only the relevant rows
            new_data = new_data.merge(
                temp_df,
                on=['id', 'age'],
                how='left',
                # If columns overlap, add '_new' to the right DataFrame's column name
                suffixes=('', '_new')
            )
            # If the merged column exists, update values where not null
            if f"{column_name}_new" in new_data.columns:
                new_data[column_name] = new_data[f"{column_name}_new"].combine_first(new_data[column_name])
                new_data.drop(columns=[f"{column_name}_new"], inplace=True)

        
        
        
        
        
        # If neither of these are true, throw an error
        else: 
            raise ValueError(f"Column {column} does not end with 'XRND' or a year. Please check the data format.")

    return new_data


    


# Function to create the child by period table from the child by age panel


def aggregate_period_data(df: pd.DataFrame, age_periods: dict) -> pd.DataFrame:
    """
    Creates a child-by-period table from a child-by-age panel, excluding the pre-birth period (age = -1) from interpolation.

    Parameters:
        df (pd.DataFrame): The input DataFrame containing child-by-age data.
        age_periods (dict): A dictionary mapping period numbers to age ranges (start_age, end_age).

    Returns:
        pd.DataFrame: A DataFrame containing interpolated values for each period (except pre-birth), where interpolation is performed by averaging values within each period. The resulting DataFrame includes columns for child ID, period, and other variables.
    """
    df = df.copy()

    # Exclude pre-birth rows (age == -1) from interpolation/aggregation
    pre_birth = df[df['age'] == -1]
    df = df[df['age'] >= 0]

    # Create bins and labels for periods, excluding pre-birth
    non_prebirth_periods = {k: v for k, v in age_periods.items() if v[0] >= 0}
    bins = [v[0] for v in non_prebirth_periods.values()]
    bins = [min(bins)] + [v[1] for v in non_prebirth_periods.values()]
    print(f"Here are the bins: {bins}")
    labels = list(non_prebirth_periods.keys())

    # Assign periods using pd.cut
    df['period'] = pd.cut(df['age'], bins=[-0.1] + bins, labels=labels, right=True)

    # Filter out rows with no period assigned (shouldn't happen, but just in case)
    df = df[df['period'].notna()]

    # Group by child and period, calculating the mean to 
    group_cols = ['id', 'period']
    value_cols = [col for col in df.columns if col not in ['id', 'age', 'period']]
    period_data = df.groupby(group_cols, as_index=False)[value_cols].mean(skipna=True)

    if not pre_birth.empty:
        pre_birth = pre_birth.copy()
        pre_birth['period'] = -1
        pre_birth = pre_birth[group_cols + value_cols]
        period_data = pd.concat([pre_birth, period_data], ignore_index=True).sort_values(['id', 'period'])

    return period_data


# --------------------------------- MAIN SCRIPT -----------------------------------------
# =======================================================================================
# =======================================================================================
# =======================================================================================
# =======================================================================================
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

nls_data = nls_data.head(20)  # Uncomment this line to limit the data for testing purposes



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


    # Combine the data with the regular NLSY79 data
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
        elif column[-4:].isdigit() or column[0:6] == "HGCREV":  # Check if the last 4 characters are digits or if the column is HGCREV
            if column[-4:].isdigit():
                year = int(column[-4:])
                # Find the column name, removing the year part (plus the underscore)
                column_name = column[:-5]
            
            else: 
                if int(column[-2:]) < 25:
                    # If the last two digits are less than 25, we assume it's a year in the 2000s
                    year = int("20" + column[-2:])
                else: 
                    year = int("19" + column[-2:])
                column_name = column[:-2]

            # Filter out unwanted prefixes from the column name
            for prefix in column_prefixes_to_remove:
                if column_name.startswith(prefix):
                    # print(f"Removing prefix '{prefix}' from column name: {column_name}")
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

            

            # print(f"Processing column with year: {year}, base column name: {column_name}")
            
            
            
            

            # Check if the column already exists in new_data
            if column_name not in new_data.columns:
                # If not, create it with NaN values
                new_data[column_name] = np.nan
            
            
            # Efficiently map mother data to child data for this column and year
            # Create a mapping from child id to (CYRB_XRND, MPUBID_XRND)
            id_to_birthyear = nls_data.set_index('id')['CYRB_XRND'].to_dict()
            id_to_motherid = nls_data.set_index('id')['MPUBID_XRND'].to_dict()
            # Create a mapping from mother id to column value
            mother_col_map = mother_data.set_index('CASEID_1979')[column].to_dict()

            # For all children, compute the child's age at this year and the value from mother data
            ids = new_data['id'].unique()
            child_ages = {id_: year - id_to_birthyear[id_] for id_ in ids}
            mother_values = {id_: mother_col_map.get(id_to_motherid[id_], np.nan) for id_ in ids}

            # Assign values in one go
            mask = new_data['age'].map(child_ages) == new_data['age']
            new_data.loc[mask, column_name] = new_data.loc[mask, 'id'].map(mother_values)


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


    # Renaming columns
    for column in new_data.columns:
        if column in better_named_columns: 
            # If the column is in the better named columns dictionary, rename it with the better name
            new_data.rename(columns={column: better_named_columns[column]}, inplace=True)


    # Rescale columns
    # Rescale columns according to rescaling_variables
    for column in new_data.columns:
        if column in rescaling_variables:
            old_values = np.arange(1, len(rescaling_variables[column]) + 1)
            new_values = rescaling_variables[column]
            new_data[column] = new_data[column].replace(dict(zip(old_values, new_values)))
            print(f"Rescaled column {column}")
    # Rescale columns according to rescaling_variables_by_age
    for entry in rescaling_variables_by_age:
        col_name, (start_age, end_age), new_values = entry
        old_values = np.arange(1, len(new_values) + 1)
        mask = (new_data['age'] >= start_age) & (new_data['age'] <= end_age)
        if col_name in new_data.columns:
            new_data.loc[mask, col_name] = new_data.loc[mask, col_name].replace(dict(zip(old_values, new_values)))
            print(f"Rescaled column {col_name} for ages {start_age}-{end_age}")

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


# 2b. Interpolating the data to fill missing values
# --- Interpolation and Filling Missing Values for Ages 0–19 ---

# Separate pre-birth (age -1) data for later recombination
pre_birth_rows = new_data[new_data['age'] == -1]

# Filter for ages 0–19
age_panel = new_data[(new_data['age'] >= 0) & (new_data['age'] <= 19)]

# Ensure every child has a complete set of ages 0–19
all_ages = pd.DataFrame({'age': np.arange(0, 20)})
# For each child, merge their data with the full age range
complete_age_panel = (
    age_panel.groupby('id', group_keys=False)
    .apply(lambda group: pd.merge(all_ages, group, on='age', how='left').assign(id=group['id'].iloc[0]))
    .reset_index(drop=True)
)

# Sort by child and age for proper interpolation
complete_age_panel = complete_age_panel.sort_values(['id', 'age'])

# Interpolate missing values for each child using cubic interpolation
def interpolate_child_data(child_df):
    # Interpolate all columns except 'id' and 'age' 
    interpolated = child_df.copy()
    value_columns = [col for col in child_df.columns if col not in ['id', 'age']]
    interpolated[value_columns] = child_df[value_columns].interpolate(
        method='cubic', limit_direction='both', axis=0
    )
    return interpolated

interpolated_panel = (
    complete_age_panel.groupby('id', group_keys=False)
    .apply(interpolate_child_data)
    .reset_index(drop=True)
)

# Fill any remaining edge NaNs by carrying forward/backward the nearest valid value
def fill_edges(child_df):
    filled = child_df.copy()
    value_columns = [col for col in child_df.columns if col not in ['id', 'age']]
    filled[value_columns] = filled[value_columns].ffill().bfill()
    return filled

final_age_panel = (
    interpolated_panel.groupby('id', group_keys=False)
    .apply(fill_edges)
    .reset_index(drop=True)
)

# Combine interpolated ages 0–19 with pre-birth rows, and sort
new_data_interpolated = pd.concat([pre_birth_rows, final_age_panel], ignore_index=True).sort_values(['id', 'age'])

# --- End Interpolation Section ---



# 3. Create the child by period table


# Create the period data
period_data = aggregate_period_data(new_data_interpolated, age_periods)


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