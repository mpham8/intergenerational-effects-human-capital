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
- scipy (for pandas under-the-hood interpolation)
- openpyxl (for pandas under-the-hood opening excel files)

Note: make sure the "categories_of_variables" file is hosted within the same directory as this file

"""

# LIBRARIES
import pandas as pd
import numpy as np
import os
from important_dictionary_variables import categories_of_variables, variable_ages, variable_descriptions



# TODO: figure out how many data points are outside manual variable ages
# TODO: trust the moms
# TODO: exclude data > 2 years outside range (above) or < 1 year outside range (below)

# Notes: 
# 1. For ages 3-5, shift down. Everything else, interpolate then remove (TODO)

# TODO: change XRND (and similarly aggregated) columns to only have one column


# CONSTANTS

# TODO: fix XRND variables

# TODO: add all variables that appear in Attanasio and Del Boca as controls

# File paths

# File path ending (it's the same across all output paths)
PATH_ENDING = "BEST"
# Input files 
nls_file_path = 'data-preprocessing/Initial_Preprocessing/07-08-25-renamed.csv'  # Update this path as needed
mother_data_file_path = 'data-preprocessing/Initial_Preprocessing/06-24-25-mother-renamed.csv'  # File containing mother data, update this path as needed
CPI_file_path = 'data-preprocessing/Initial_Preprocessing/historical-cpi-u-202505.xlsx'

# Output files
nan_file_path = f'data-preprocessing/Processed_Data/nan_columns_{PATH_ENDING}.csv'  # File to save columns with NaN values for further investigation
age_output_file_path = f'data-preprocessing/Processed_Data/child_age_panel_{PATH_ENDING}.csv'
period_output_file_path = f'data-preprocessing/Processed_Data/child_period_panel_{PATH_ENDING}.csv'  # File to save the child by period data
period_wide_output_file_path = f'data-preprocessing/Processed_Data/child_period_panel_wide_{PATH_ENDING}.csv'  # File to save the wide format of the child by period data


# Defining terms for processing
SHORTEN_DATA = False
NUMBER_OF_ROWS_TESTING = 500
PREBIRTH_AGES_PER_CHILD = 2 # determining how many pre-birth ages I want to keep (to backfill in case -1 is unavailable)


# Defining terms for rescaling data
SEVERAL_TIMES_PER_YEAR = 7
SEVERAL_TIMES_PER_MONTH = 5
SEVERAL_TIMES_PER_WEEK = 4
MORE_THAN_ONCE_PER_DAY = 2
WEEKS_PER_MONTH = 4.345

INFLATION_ADJUSTED_COLUMNS = ["TNFI_TRUNC", "TOTAL_FAMILY_INCOME_FR_ALL", "FAMILY_INCOME_FROM_ALL_SOUR", "LABOR_INCOME"]



# Age periods dictionary
age_periods = {
    -1: (-1, -1), # Pre-birth
    0: (0, 5), # Pre-elementary
    1: (6, 9), # Elementary
    2: (10, 14), # Secondary
    3: (15, 19) # High school
}

columns_to_drop = {
    # "Q2_15C", 
    "Q2_15A_PRE", 
    "VERSION_R29_XRND", 
    "TYPE_OF_SCHOOL_RECODE", 
    "MOM_HELPS_CH_W_NONE", 
    "MOM_SAMPLE_ID", 
    "MOM_SEX", 
    "DOES_CHILD_NEVER_USE", 
    'TOTAL_FAMILY_INCOME_FR_ALL', 
    'FAMILY_INCOME_FROM_ALL_SOUR', 

}


# List of prefixes for columns that should be removed when naming columns
column_prefixes_to_remove = [
    'RC_HOME_B_',
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
    'CHECK_',  
    'MS_', 
]

# Dictionary to map poorly-named columns to their intended names (basically, handling exceptions in the CLNS79 data)
poorly_named_columns = {
    'MOM_HELPS_CH_LE': 'MOM_HELPS_CH_LEARN_NUMBERS',
    'MOM_HELPS_CH_LEA': 'MOM_HELPS_CH_LEARN_NUMBERS',
    'MOM_HELPS_CH_LEAR' : 'MOM_HELPS_CH_LEARN_NUMBERS',
    'MOM_HELPS_CH_LEARN_N': 'MOM_HELPS_CH_LEARN_NUMBERS',
    'MOM_HELPS_CH_LEARN_A': 'MOM_HELPS_CH_LEARN_ALPHABET',
    'MOM_HELPS_CH_LEARN_ALPHABE' : 'MOM_HELPS_CH_LEARN_ALPHABET',
    'MOM_HELPS_CH_LEARN_C': 'MOM_HELPS_CH_LEARN_COLORS',
    'MOM_HELPS_CH_LEARN_S': 'MOM_HELPS_CH_LEARN_SHAPES',
    'MOM_HELPS_CH_W_N': 'MOM_HELPS_CH_W_NONE',
    'SCHOOL_CHILD_ATTENDS': 'TYPE_OF_SCHOOL',
    'TYPE_OF_SCHOOL_94': 'TYPE_OF_SCHOOL',
    'TYPE_OF_SCHOOL_96': 'TYPE_OF_SCHOOL',
    # NOTE: confused, what's the difference between "C" and "Y"
    'TYPE_OF_SCHOOL_CHILD_ATT' : 'TYPE_OF_SCHOOL',
    'TYPE_OF_SCH_CHD_ATTNDS_C' : 'TYPE_OF_SCHOOL',
    'TYPE_OF_SCHOOL_CHILD_ATTEND' : 'TYPE_OF_SCHOOL',
    'TYPE_SCHOOL_CHILD_ATTENDS_V' : 'SCHOOL_CHILD_ATTENDS_RECD', 
    'CH_ATTENDS_PUBLIC_PRIV_R' : 'CH_ATTENDS_PUBLIC_PRIV_RELI', 
    'CHILD_ATTENDS_PUBLIC_PRIV_R' : 'CH_ATTENDS_PUBLIC_PRIV_RELI', 
    'IS_CURRENT_MOST_RECENT_SCHO' : 'CH_ATTENDS_PUBLIC_PRIV_RELI', 
    'IS_SCHOOL_GIFTED_HANDICA' : 'IS_SCHOOL_GIFTED_HANDICAPPE', 
    'CHILD_S_AGE_WHEN_1ST_ATTD_H': 'CHILD_AGE_WHEN_1ST_ATTD_HEA', 
    'HOW_LONG_DID_CHILD_ATTEND_H' : 'HOW_LONG_CHILD_WAS_IN_HEAD', 
    'HOW_LONG_CHILD_ATTENDED_HEA' : 'HOW_LONG_CHILD_WAS_IN_HEAD', 
    'CHILD_EVER_ENRLD_IN_HEAD_ST' : 'CHILD_EVER_ENROLLED_IN_HEAD', 
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
    'HOW_OFT_CH_TAKN_TO_P' : 'HOW_OFT_TAKEN', 
    'HOW_OFT_CH_TAKE_TO_P' : 'HOW_OFT_TAKEN',
    'MUSIC_INSTMT_CH_CA' : 'MUSIC_INSTMT_CH',
    'MUSIC_INSTMT_C' : 'MUSIC_INSTMT_CH',
    'IS_THERE_MUSIC_INSTR' : 'MUSIC_INSTMT_CH',
    'MOM_HELP_MORE_W_SCHO' : 'IF_LOW_GRADES', 
    'IF_LOW_GRADES_HE' : 'IF_LOW_GRADES',
    'IF_LOW_GRADES_HEL' : 'IF_LOW_GRADES', 
    'HOW_OFTEN_PARS_HELP_R_WITH' : 'HOW_OFT_PARENTS_HELP_WITH_H', 
    'IN_SCL_YR_HOW_OFTEN_PARENTS' : 'HOW_OFT_PARENTS_HELP_WITH_H',
    'SCHOOL_HOW_OFTEN_PARENTS_HE' : 'HOW_OFT_PARENTS_HELP_WITH_H',
    'DO_PARS_DISCUS' : 'DO_PARS_DISCUSS_TV',
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
    'CH_GET_SPEC_LE' : 'CH_GET_SPEC_LESSON',
    'CH_GET_SPEC_LESS' : 'CH_GET_SPEC_LESSON',
    'CH_GET_SPEC_LESSO' : 'CH_GET_SPEC_LESSON',
    'TOTAL_FAMILY_INCOME_FROM_AL' : 'TOTAL_FAMILY_INCOME_FR_ALL',
    'MAR_10A' : 'Q2_15A',
    'MAR_10B' : 'Q2_15B',
    'MUSICAL_INSTMT_CH' : 'MUSIC_INSTMT_CH', 
    'CH_GET_SPEC_LESSONS' : 'CH_GET_SPEC_LESSON', 
    'HIGHEST_GRADE_R_HAS_COMPLET' : 'HIGHEST_GRADE_OF_REGULAR_SC', 
    'Q13_5_TRUNC' : 'Q13_5', 
    'Q13_5_TRUNC_REVISED' : 'Q13_5', 
    
    # NOTE: combine Q2_15A and Q2_15A_PRE?
}

better_named_columns = {
    'HOW_OFT_CH_TAKEN' : 'HOW_OFT_CH_TAKEN_TO_MUSEUM', 
    'HOW_OFT_TAKEN' : 'HOW_OFT_CH_TAKEN_TO_PERFORMANCE',
    'HOW_OFT_CH_SPEND' : 'HOW_OFT_CH_SPEND_TIME_W_DAD',
    'HOW_OFT_CH_W_DAD' : 'HOW_OFT_CH_W_DAD_OUTDOORS',
    'SAMPLE_RACE_78SCRN' : 'MOM_RACE',
    'SCHOOL_CHILD_ATTENDS_RECD' : 'TYPE_OF_SCHOOL_RECODE', 
    'Q2_15A' : 'SPOUSE_WKSWK_PCY', 
    'Q2_15B' : 'SPOUSE_HRSWK_PCY',
    'SAMPLE_ID' : 'MOM_SAMPLE_ID', 
    'SAMPLE_SEX' : 'MOM_SEX', # could be a good check
    'HGCREV' : 'HGC_REV_MOM', 
    'IS_SCHOOL_GIFTED_HANDICAPPE' : 'IS_SCHOOL_GIFTED_HANDICAPPED',
    'HOW_OFT_PARENTS_HELP_WITH_H' : 'PARS_HELP_W_HOMEWORK',
    'PIAT_MATH_TOTAL_RAW_SCORE' : 'PIAT_MATH', 
    'PIAT_READ_REC_TOTAL_RAW_SCO' : 'PIAT_READ_REC', 
    'PIAT_READ_COMP_TOTAL_RAW_SC' : 'PIAT_READ_COMP', 
    'PPVT_TOTAL_RAW_SCORE' : 'PPVT', 
    'HIGHEST_GRADE_OF_REGULAR_SC' : 'HGC_YEARLY_CHILD', # NOTE: does this column need to be processed differently? It's the only column in "Educational Attainment" that's not XRND (i.e., across the whole file)
    'Q13_5' : 'LABOR_INCOME', 
    'HGC_EVER_XRND' : 'HGC_EVER_MOM_XRND', 
    'HIGHEST_DEGREE_EVER_XRND' : 'HIGHEST_DEGREE_EVER_MOM_XRND',
}

# If over time, all variables will be rescaled to per week (easiest to do)
rescaling_variables = {
    'HOW_OFTEN_MOM_READS' : [0, SEVERAL_TIMES_PER_YEAR/52, SEVERAL_TIMES_PER_MONTH/WEEKS_PER_MONTH, 1, 3, 7], 
    'HOW_OFT_CH_EATS_W' : [MORE_THAN_ONCE_PER_DAY*7, 7, SEVERAL_TIMES_PER_WEEK, 1, 1/WEEKS_PER_MONTH, 0], 
    'HOW_OFT_CH_TAKEN_TO_MUSEUM' : [0, 1.5/52, SEVERAL_TIMES_PER_YEAR/52, 1/WEEKS_PER_MONTH, 1],
    'HOW_OFT_CH_TAKEN_TO_PERFORMANCE' : [0, 1.5/52, SEVERAL_TIMES_PER_YEAR/52, 1/WEEKS_PER_MONTH, 1],
    'HOW_OFT_CH_W_DAD' : [7, 4, 1, 1/WEEKS_PER_MONTH, 3/52, np.nan], 
    'HOW_OFT_CH_W_DAD_OUTDOORS' : [7, 4, 1, 1/WEEKS_PER_MONTH, 3/52, np.nan],
    'DO_PARS_DISCUSS_TV' : [0, 1, np.nan], 
    'PARS_HELP_W_HOMEWORK' : [0.5/WEEKS_PER_MONTH, 1.5/WEEKS_PER_MONTH, 1.5, 5, 7, np.nan], 
    # This variable below will be rescaled to months
    'HOW_LONG_CHILD_WAS_IN_HEAD': [(0+3)/2, (3+11)/2, (12+23)/2, 24, np.nan], # NOTE: need help with rescaling

    # The variables below should be binary
    'MOM_HELPS_CH_LEARN_NUMBERS': [1, 1, 1, 1, 1], 
    'MOM_HELPS_CH_LEARN_ALPHABET': [1, 1, 1, 1, 1], 
    'MOM_HELPS_CH_LEARN_SHAPES': [1, 1, 1, 1, 1], 
    'MOM_HELPS_CH_LEARN_COLORS': [1, 1, 1, 1, 1], 
    'MOM_HELPS_CH_W_NONE': [1, 1, 1, 1, 1],
    'DO_PARS_DISCUSS_TV': [1, np.nan, np.nan, np.nan, np.nan, np.nan],
    
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
        pd.DataFrame: A DataFrame containing child-by-age data, with columns for child ID, age, year, and other variables.
    """
    # A row in our new dataframe might look like this:
    # | id | age | Year | math_score | reading_score | ... |

    new_data = pd.DataFrame()
    # Create the 'id' column such that it contains 20+PREBIRTH_ unique child IDs for every child in the NLSY79 data (one for each age from -1 to 19)
    new_data['id'] = np.repeat(nls_data['id'].unique(), 20+PREBIRTH_AGES_PER_CHILD)
    # Create the 'age' column such that it contains the ages from 0 to 19 for each child
    new_data['age'] = np.tile(np.arange(-PREBIRTH_AGES_PER_CHILD, 20), len(nls_data['id'].unique()))
    # Initialize the Year column with NaN
    new_data['year'] = np.nan

    print(new_data.head())
    for column in nls_data.columns:
        # print(f"Processing column: {column}")
        # Skip the 'id' column

        if column == 'id':
            continue
        
        # If the column ends in "XRND," there is no specific date attached to it, and we can fill it in for all ages
        if column.endswith('XRND'):
            new_data[column] = np.repeat(nls_data[column].values, 20+PREBIRTH_AGES_PER_CHILD)  # Repeat the values for each age
            
                
        
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
            



            # If the column is 'HGC_OF_MOTHER_AS_OF_MAY_1_R_1994', we should exclude (it's a strange anomaly in the data)
            if column == 'HGC_OF_MOTHER_AS_OF_MAY_1_R_1994':
                continue


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
            age_values = nls_data['age'].values
            col_values = nls_data[column].values

            # Create a DataFrame for merging
            temp_df = pd.DataFrame({
                'id': id_values,
                'age': age_values,
                column_name: col_values,
                'year': year
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
            # Update the Year column only where it is NaN and temp_df.Year is not NaN
            if "year_new" in new_data.columns:
                new_data['year'] = new_data['year'].combine_first(new_data['year_new'])
                new_data.drop(columns=['year_new'], inplace=True)

        
        # TODO: check variables for interpolation (only interpolate with min_age and max_age + 1)
        
        
        
        # If neither of these are true, throw an error
        else: 
            raise ValueError(f"Column {column} does not end with 'XRND' or a year. Please check the data format.")

    return new_data


    


# Function to create the child by period table from the child by age panel


def aggregate_period_data(df: pd.DataFrame, age_periods: dict) -> pd.DataFrame:
    """
    Creates a child-by-period table from a child-by-age panel.
    For variables in categories_of_variables["Child_Human_Capital"], aggregates using the value at the END of each period.
    For all other variables, aggregates using the mean over the period.

    Parameters:
        df (pd.DataFrame): The input DataFrame containing child-by-age data.
        age_periods (dict): A dictionary mapping period numbers to age ranges (start_age, end_age).

    Returns:
        pd.DataFrame: A DataFrame containing values for each period, with Child_Human_Capital variables taken at the period end.
    """
    df = df.copy()
    # Remove 'year' column if present
    if 'year' in df.columns:
        df = df.drop('year', axis=1)

    # Get Child_Human_Capital variable list
    chc_vars = set(categories_of_variables.get("Child_Human_Capital", []))

    # Exclude pre-birth rows (age == -1) from aggregation
    pre_birth = df[df['age'] == -1]
    df = df[df['age'] >= 0]

    # Prepare period bins and labels (excluding pre-birth)
    non_prebirth_periods = {k: v for k, v in age_periods.items() if v[0] >= 0}
    bin_edges = []
    for v in non_prebirth_periods.values():
        bin_edges.append(v[0])
        bin_edges.append(v[1] + 1)
    bin_edges = sorted(set(bin_edges))
    min_age = 0
    max_age = int(df['age'].max()) + 1
    if bin_edges[0] > min_age:
        bin_edges = [min_age] + bin_edges
    if bin_edges[-1] < max_age:
        bin_edges = bin_edges + [max_age]
    bin_edges = sorted(set(bin_edges))
    period_labels = list(non_prebirth_periods.keys())

    # Assign periods
    df['period'] = pd.cut(df['age'], bins=bin_edges, labels=period_labels, right=False, include_lowest=True)
    df = df[df['period'].notna()]

    group_cols = ['id', 'period']
    value_cols = [col for col in df.columns if col not in ['id', 'age', 'period']]

    # Prepare result DataFrame
    result_rows = []

    # For each child and period, aggregate accordingly
    for (child_id, period), group in df.groupby(['id', 'period']):
        period = int(period)
        period_info = age_periods[period]
        period_end_age = period_info[1]
        row = {'id': child_id, 'period': period}

        # For Child_Human_Capital variables: take value at period end age
        for col in value_cols:
            if col in chc_vars:
                val = group.loc[group['age'] == period_end_age, col]
                if not val.empty:
                    row[col] = val.iloc[0]
                else:
                    # If no value at target_age, fallback to nearest available in period
                    if not group[col].dropna().empty:
                        # Use value at max available age in period
                        max_age = group.loc[group[col].notna(), 'age'].max()
                        row[col] = group.loc[group['age'] == max_age, col].iloc[0]
                    else:
                        row[col] = np.nan
            else:
                # For other variables: mean over the period
                row[col] = group[col].mean()
        result_rows.append(row)

    period_data = pd.DataFrame(result_rows)

    # Handle pre-birth period (-1): use age=0 for Child_Human_Capital variables
    if not pre_birth.empty:
        pre_birth_rows = []
        for child_id, group in pre_birth.groupby('id'):
            row = {'id': child_id, 'period': -1}
            for col in value_cols:
                row[col] = group[col].iloc[0]
            pre_birth_rows.append(row)
        pre_birth_df = pd.DataFrame(pre_birth_rows)
        period_data = pd.concat([pre_birth_df, period_data], ignore_index=True)

    period_data = period_data.sort_values(['id', 'period']).reset_index(drop=True)
    return period_data


def transform_period_data(age_panel: pd.DataFrame, period_panel: pd.DataFrame) -> pd.DataFrame:
    """ 
    Transforms the period data from long to wide format using both the child-age panel and the child-period panel.
    - For Government_Inputs: one column, latest value (largest age) from age_panel
    - For Educational_Attainment: one column, latest value (largest age) from age_panel
    - For Parent_Characteristics: one column, earliest value (smallest age) from age_panel
    - All other variables: wide format, one column per period (within variable's age range) from period_panel
    """
    id_col = 'id'
    period_col = 'period'
    value_cols = [col for col in period_panel.columns if col not in [id_col, period_col]]

    gov_inputs = set(categories_of_variables.get("Government_Inputs", []))
    edu_attain = set(categories_of_variables.get("Educational_Attainment", []))
    parent_chars = set(categories_of_variables.get("Parent_Characteristics", []))
    other_vars = set(categories_of_variables.get("Other", []))

    # Creating copies (in case this is messing up the period data somehow?)
    age_panel = age_panel.copy()
    period_panel = period_panel.copy()

    # 1. Special columns (single column per variable) from age_panel
    result = age_panel[[id_col]].drop_duplicates().reset_index(drop=True)

    # Government_Inputs: latest value (largest age)
    for col in gov_inputs.intersection(age_panel.columns):
        temp = age_panel[[id_col, 'age', col]].copy()
        temp = temp.loc[temp[col].notna()]
        idx = temp.groupby(id_col)['age'].idxmax()
        result[col] = temp.loc[idx].set_index(id_col)[col].reindex(result[id_col]).values

    # Educational_Attainment: latest value (largest age)
    for col in edu_attain.intersection(age_panel.columns):
        temp = age_panel[[id_col, 'age', col]].copy()
        temp = temp.loc[temp[col].notna()]
        idx = temp.groupby(id_col)['age'].idxmax()
        result[col] = temp.loc[idx].set_index(id_col)[col].reindex(result[id_col]).values

    # Parent_Characteristics: earliest value (smallest age)
    for col in parent_chars.intersection(age_panel.columns):
        temp = age_panel[[id_col, 'age', col]].copy()
        temp = temp.loc[temp[col].notna()]
        idx = temp.groupby(id_col)['age'].idxmin()
        result[col] = temp.loc[idx].set_index(id_col)[col].reindex(result[id_col]).values
    
    # Other: any value (should be the same for all ages)
    for col in other_vars.intersection(age_panel.columns):
        if col == id_col:
            continue
        temp = age_panel[[id_col, col]].copy()
        temp = temp.loc[temp[col].notna()]
        # Just take the first non-null value for each id
        result[col] = temp.groupby(id_col)[col].first().reindex(result[id_col]).values

    # 2. All other variables: wide format, one column per period (within variable's age range) from period_panel
    keep_pairs = []
    for col in value_cols:
        if col in gov_inputs or col in edu_attain or col in parent_chars or col in other_vars:
            continue
        min_age, max_age = variable_ages.get(col, (None, None))
        if min_age is None or max_age is None:
            continue
        for period in sorted(period_panel[period_col].unique()):
            period_info = age_periods.get(int(period))
            if period_info is None:
                continue
            period_start, period_end = period_info
            if (period_end >= min_age) and (period_start <= max_age):
                keep_pairs.append((col, period))

    # Pivot to wide format, keeping only valid (variable, period) pairs
    wide_df = period_panel.pivot(index=id_col, columns=period_col)
    wide_df.columns = [f"{col}_P{int(period)}" for col, period in wide_df.columns]
    wide_df = wide_df.reset_index()

    # Filter columns to only those in keep_pairs
    valid_colnames = [f"{col}_P{int(period)}" for col, period in keep_pairs]
    result_cols = [id_col] + [c for c in wide_df.columns if c in valid_colnames]
    wide_part = wide_df[result_cols]

    # Merge special columns and wide columns
    final = result.merge(wide_part, on=id_col, how='left')
    return final

def get_CPI_values(CPI_file_path: str) -> np.ndarray:
    CPI_data = pd.read_excel(CPI_file_path, header=3)
    CPI_data['yearly_data'] = CPI_data.iloc[:, 2:].sum(axis=1, numeric_only=True)
    return dict(zip(CPI_data['Year'].values, CPI_data['yearly_data']))


def get_age_range(df: pd.DataFrame, column: str) -> tuple:
    """
    Returns the (min_age, max_age) for which the specified column has non-NaN and non-negative values in the child-age panel.
    If all values are NaN or negative, returns (None, None).
    """
    valid_ages = df.loc[df[column].notna() & (df[column] >= 0), 'age']
    if valid_ages.empty:
        return (None, None)
    return (valid_ages.min(), valid_ages.max())


# --------------------------------- MAIN SCRIPT -----------------------------------------
# =======================================================================================
# =======================================================================================
# =======================================================================================
# =======================================================================================
# =======================================================================================

def main():


    # 0. Deleting any existing output files to avoid confusion

    query = input("Would you like to delete any existing output files? (yes/no): ").strip().lower()
    # List of output files to delete
    output_files = [nan_file_path, age_output_file_path, period_output_file_path, period_wide_output_file_path]
    if query == 'yes':
        # Deleting the output files if they exist
        for file in output_files:
            if os.path.exists(file):
                os.remove(file)
                print(f"Deleted file: {file}")
            else:
                print(f"File not found, skipping deletion: {file}")
    elif query == 'kill' or query == 'quit': 
        # Quitting program
        print("Killing program....")
        quit()
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
    if SHORTEN_DATA: 
        nls_data = nls_data.head(NUMBER_OF_ROWS_TESTING)
        print(f"Data shortened to {NUMBER_OF_ROWS_TESTING} rows")




    # 2. Create the child by age panel

    # Creating the basic panel, excluding special columns (of which there are none right now)
    new_data = create_child_by_age_panel(nls_data)



    # Combine the data with the regular NLSY79 data
    try:
        mother_data = pd.read_csv(mother_data_file_path)
    except FileNotFoundError:
        print(f"Error: The file {nls_file_path} was not found. Please check the file path.")
        raise

    mother_data = pd.DataFrame(mother_data)

    for column in mother_data.columns:
        
        # print(f"Processing column: {column}")
        # Skip the 'id' column
        if column == 'CASEID_1979':
            continue
            

        # If the column ends in a date (e.g. 1979, 1980, etc.), we need to find the age of the child at that date
        elif column[-4:].isdigit():  # Check if the last 4 characters are digits
            if column[-4:].isdigit():
                year = int(column[-4:])
                # Find the column name, removing the year part (plus the underscore)
                column_name = column[:-5]
            
            # Remove any lingering dates
            if column_name[-2:].isdigit() and not column_name.startswith("ASVAB"):
                # Find the column name, removing the year part (plus the underscore)
                column_name = column_name[:-2]

            # Filter out unwanted prefixes from the column name
            for prefix in column_prefixes_to_remove:
                if column_name.startswith(prefix):
                    # print(f"Removing prefix '{prefix}' from column name: {column_name}")
                    # Remove the prefix from the column name
                    column_name = column_name[len(prefix):]

            # If the column name is in the poorly named columns dictionary, rename it with the better name
            if column_name in poorly_named_columns:
                column_name = poorly_named_columns[column_name]
            




            # # FOR TESTING PURPOSES: continue if not in the special columns for testing
            # if column_name not in special_columns_for_testing:
            #     # print(f"Skipping column {column} as it is not in the special columns for testing.")
            #     continue

            
            
            
            
            

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
            mask = new_data['age'] == new_data['id'].map(child_ages)
            new_data.loc[mask, column_name] = new_data.loc[mask, 'id'].map(mother_values)


            # Ensure we have a pre-birth age (-1) value for each child for the column
            if new_data[(new_data['age'] == -1) & (new_data[column_name].notna())].empty:
                # If there is no value for age -1, we will fill it in with age -2, or -3, and so on
                # Get the greatest negative age that has a value for the column (i.e., the most recent age before birth)
                negative_ages = new_data[(new_data['age'] < 0) & (new_data[column_name] >= 0)]['age']
                if not negative_ages.empty:
                    greatest_negative_age = negative_ages.max()
                    # Fill in the value for the corresponding age -1
                    for id in new_data['id'].unique():
                        new_data.loc[(new_data['id'] == id) & (new_data['age'] == -1), column_name] = new_data.loc[(new_data['id'] == id) & (new_data['age'] == greatest_negative_age), column_name].values[0]
                

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
    print("Rescaling columns...")
    for column in new_data.columns:
        if column in rescaling_variables:
            old_values = np.arange(1, len(rescaling_variables[column]) + 1)
            new_values = rescaling_variables[column]
            new_data[column] = new_data[column].replace(dict(zip(old_values, new_values)))
            # print(f"Rescaled column {column}")

    # Rescale columns according to rescaling_variables_by_age
    for entry in rescaling_variables_by_age:
        col_name, (start_age, end_age), new_values = entry
        old_values = np.arange(1, len(new_values) + 1)
        mask = (new_data['age'] >= start_age) & (new_data['age'] <= end_age)
        if col_name in new_data.columns:
            new_data.loc[mask, col_name] = new_data.loc[mask, col_name].replace(dict(zip(old_values, new_values)))
            # print(f"Rescaled column {col_name} for ages {start_age}-{end_age}")


    # Rescale columns according to inflation adjustment
    CPI_values = get_CPI_values(CPI_file_path)
    for column in new_data.columns: 
        if column in INFLATION_ADJUSTED_COLUMNS: 
            # Rescale the column by the CPI value
            new_data[column] = new_data.apply(
                lambda row: row[column] * (CPI_values.get(row['year'], 1) / CPI_values.get(1979, 1)),
                axis=1
            )


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
        
        # Drop all NaN columns (for now)
        new_data = new_data.drop(all_nan_columns, axis=1)

    else:
        print("No columns with all NaN values found in the new_data DataFrame.")


    # Checking for columns that may be duplicates of each other
    print("\nChecking for duplicate columns...")
    for col1 in new_data.columns: 
        for col2 in new_data.columns: 
            if col1 != col2 and (col1.startswith(col2)): 
                print(f"Warning: should {col1} and {col2} be the same column?")
    print()
    # Save the new_data DataFrame to a new file
    new_data.to_csv(age_output_file_path, index=False)
    print(f"Cleaned data saved to {age_output_file_path}")


    # Create a file to store the age ranges for the variables
    with open(f"data-preprocessing/Initial_Preprocessing/variable_ages.txt", "w") as f:
        f.write("variable_ages = { \n")
        for column in new_data.columns: 
            min_age, max_age = get_age_range(new_data, column)
            f.write(f"{column} : ({min_age}, {max_age}) \n")

        f.write("}")



    # 2b. Interpolating the data to fill missing values
    # --- Interpolation and Filling Missing Values for Ages 0–19 ---

    print("Interpolating data...")
    # Separate pre-birth (age -1) data from post-birth (age 0-19) data for later recombination
    pre_birth_rows = new_data[new_data['age'] == -1].copy()
    age_panel = new_data[(new_data['age'] >= 0) & (new_data['age'] <= 19)].copy()



    # Replace all negative values with np.nan EXCEPT the "-1" values in the "age" column
    for col in new_data.columns:
        if col == 'age':
            continue
        else:
            pre_birth_rows.loc[pre_birth_rows[col] < 0, col] = np.nan
            age_panel.loc[age_panel[col] < 0, col] = np.nan
    print(f"Replaced all negative values with NaN, except -1 in the 'age' column")

    # Fixing any incorrectly inputted data
    # TODO: deciding what to do with the max_age + 1 values
    # For ages 3-5 variables, if max_age + 1 is filled, shift all values down by one (so 4 -> 3, 6 -> 5, etc.)
    # For all other variables, just use the interpolated value (done after interpolation)
    for col in age_panel.columns:
        if col in ['id', 'age']: 
            continue
        if variable_ages[col] == (3, 5): 
            
            for id_ in age_panel['id'].unique(): 
                if 6 in age_panel.loc[(age_panel["id"] == id_), 'age'].values:
                    value_to_move_6 = age_panel.loc[(age_panel["age"] == 6) & (age_panel["id"] == id_), col].values[0]
                    value_to_move_4 = age_panel.loc[(age_panel["age"] == 4) & (age_panel["id"] == id_), col].values[0]
                    if value_to_move_6 != np.nan: 
                        age_panel.loc[(age_panel["age"] == 5) & (age_panel["id"] == id_), col] = value_to_move_6
                        if value_to_move_4 != np.nan: 
                            age_panel.loc[(age_panel["age"] == 3) & (age_panel["id"] == id_), col] = value_to_move_4
            
        
    # Sort by child and age for proper interpolation
    age_panel = age_panel.sort_values(['id', 'age'])

    # Interpolate missing values for each child using cubic interpolation if possible, otherwise linear
    def interpolate_child_data(child_df):
        interpolated = child_df.copy()
        value_columns = [col for col in child_df.columns if col not in ['id', 'age']]
        # Use cubic if enough points, else fallback to linear
        for col in value_columns:
            non_nan_count = child_df[col].notna().sum()

            if non_nan_count >= 2:
                interpolated[col] = child_df[col].interpolate(method='linear', limit_direction='both', limit_area='inside')
            elif non_nan_count == 1:
                interpolated[col] = child_df[col]
            else: 
                interpolated[col] = np.nan
        return interpolated

    interpolated_panel = (
        age_panel.groupby('id', group_keys=False)
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
    print("Interpolated data. Here are the first few rows of the dataframe")
    print(new_data_interpolated.head())
    
        
    # Limiting the data between min and max age, replacing all other values with NaN
    for col in new_data_interpolated.columns:
        if col in ['id', 'age']:
            continue
        min_age, max_age = variable_ages[col]
        new_data_interpolated.loc[(new_data_interpolated['age'] < min_age) | (new_data_interpolated['age'] > max_age), col] = np.nan

    
    # Counting the number of people who are not in the age range for each variable
    print("Checking for out-of-range ages in interpolated data...")
    for col in new_data_interpolated.columns:
        if col in ['id', 'age']:
            continue
        min_age, max_age = variable_ages[col]
        out_of_range_count = new_data_interpolated[(new_data_interpolated['age'] < min_age) | (new_data_interpolated['age'] > max_age)][col].notna().sum()
        if out_of_range_count > 0:
            print(f"Warning: Column '{col}' has {out_of_range_count} values outside the age range ({min_age}, {max_age}).")
    print("Out-of-range age check completed.")

            
    # --- End Interpolation Section ---

    

    # DEBUGGING: testing whether there are negative values in this part of the data
    for col in new_data_interpolated.columns:
        if (not new_data_interpolated.loc[new_data_interpolated[col] < 0, col].empty) and (col != 'age'): 
            print(f"Column {col} contains negative values from interpolation!")

    # Create a graph to show how interpolation is giving negative values
    # import matplotlib.pyplot as plt

    # column_to_examine = 'PIAT_MATH'
    # child_to_examine = new_data_interpolated.loc[new_data_interpolated[column_to_examine] < 0, 'id'].values[0]
    # x = new_data_interpolated.loc[new_data_interpolated['id'] == child_to_examine]['year'].values
    # y = new_data_interpolated.loc[new_data_interpolated['id'] == child_to_examine][column_to_examine].values
    # actual_points = new_data.loc[new_data['id'] == child_to_examine][column_to_examine].values
    # plt.plot(x, y)
    # plt.scatter(x, actual_points)
    # plt.show()

    


    # 3. Create the child by period table
    

    # Create the period data
    period_data = aggregate_period_data(new_data_interpolated, age_periods)


    # Dropping bad columns
    period_data = period_data.drop(columns=columns_to_drop)

    
    # Counting the number of people who are not in the age range for each variable
    print("Checking for out-of-range periods in interpolated data...")
    for col in period_data.columns:
        if col in ['id', 'period']:
            continue
        min_age, max_age = variable_ages[col]
        acceptable_periods = []
        for period, (min_age_period, max_age_period) in age_periods.items(): 
            if min_age <= max_age_period and max_age >= min_age_period: 
                acceptable_periods.append(period)
        # print(f"The acceptable periods for {col} are between {min(acceptable_periods)} and {max(acceptable_periods)} (inclusive)")
        out_of_range_count = period_data.loc[(period_data['period'] < min(acceptable_periods)) | (period_data['period'] > max(acceptable_periods)), col].notna().sum()
        if out_of_range_count > 0:
            print(f"Warning: Column '{col}' has {out_of_range_count} values outside the age range ({min_age}, {max_age}).")

    print("Out-of-range age check completed.")



    # Print the first few rows of the period data to verify
    print("Created period data. Here are the first few rows:")
    print(period_data.head())


    # Print all the columns in the period data
    print("\nColumns in the period data:")
    print(period_data.columns.tolist())

    # Describe the period data
    print("\nDescribing the period data")
    print(period_data.describe())
    # 3a. Running Checks



    
    
    # Checking to see if there are any columns with all NaN values
    nan_counts = period_data.isna().sum()
    all_nan_columns = nan_counts[nan_counts == len(period_data)].index.tolist()
    if all_nan_columns:
        print("Warning: The following columns have all NaN values in the period_data DataFrame:")
        print(all_nan_columns)
        
        # Drop all NaN columns (for now)
        period_data = period_data.drop(all_nan_columns, axis=1)

    else:
        print("No columns with all NaN values found in the period_data DataFrame.")

    

    # Checking for "bad" columns

    # Count the number of full rows gained from dropping each column
    # gain_from_column_drop = {}
    # initial_full_rows = period_data.copy().dropna(inplace=False).shape[0]
    # print(f"Initial full rows: {initial_full_rows}")
    # # Exclude 'id' and 'period' columns from the loop
    # columns_to_check = [col for col in period_data.columns if col not in ['id', 'period']]
    # for col in columns_to_check:
    #     dropped = period_data.copy().drop(columns=[col])
    #     full_rows = dropped.dropna().shape[0]
    #     gain_from_column_drop[col] = full_rows - initial_full_rows
    # print("Rows gained from dropping each column (full rows only):")
    # results = []
    # for col, gain in gain_from_column_drop.items():
    #     results.append(f"{col}: {gain}")
    # print("\n".join(results))
    

    




    # Save data to csv
    period_data.to_csv(period_output_file_path, index=False)
    print(f"Period data saved to {period_output_file_path}")

    # Save description stats to csv
    # print(period_data.describe(include='all'))  # Uncomment for debugging if needed
    period_data_stats = period_data.describe(include='all').transpose()
    period_data_stats["Category"] = ""
    period_data_stats["Description"] = ""
    # Assign categories and descriptions to period data columns
    for category, column_names in categories_of_variables.items(): 
        for column in column_names: 
            if column not in columns_to_drop and column in period_data.columns: 
                period_data_stats.loc[column, "Category"] = category
                period_data_stats.loc[column, "Description"] = variable_descriptions.get(column, "No description available")
            else: 
                print(f"Warning: column {column} not in period_data.columns")
    period_data_stats.to_csv(f"{period_output_file_path[:-4]}_Descriptive_Stats.csv")
    print(f"Summary of period data saved to {period_output_file_path[:-4]}_Descriptive_Stats.csv")
    print("Summary of period data saved")

    # Transform the period data to wide format
    period_data_wide = transform_period_data(new_data_interpolated, period_data)
    period_data_wide.to_csv(period_wide_output_file_path, index=False)
    print(f"Wide period data saved to {period_wide_output_file_path}")

    # Print and save description stats to csv
    print("\nSummary of wide period data:")
    print(period_data_wide.describe(include='all'))
    period_data_wide_stats = period_data_wide.describe(include='all').transpose()
    period_data_wide_stats["Category"] = ""
    period_data_wide_stats["Description"] = ""

    # Assign categories and descriptions to wide period data columns
    for category, column_names in categories_of_variables.items():
        for column in column_names:
            # Find all wide columns that start with the variable name (handles suffixes like _P0, _P1, etc.)
            matching_cols = [c for c in period_data_wide.columns if c.startswith(f"{column}_")]
            for wide_col in matching_cols:
                if wide_col not in columns_to_drop:
                    idx = period_data_wide_stats.index.get_loc(wide_col)
                    period_data_wide_stats.at[wide_col, "Category"] = category
                    # Add description, with note if it's a period column
                    base_desc = variable_descriptions.get(column, "No description available")
                    # Extract period info from column name
                    if "_P" in wide_col:
                        period_num = wide_col.split("_P")[-1]
                        period_note = f" Period {period_num}: "
                        period_data_wide_stats.at[wide_col, "Description"] = period_note + base_desc
                    else:
                        period_data_wide_stats.at[wide_col, "Description"] = base_desc

    # Also assign categories and descriptions for columns without suffixes (single columns)
    for category, column_names in categories_of_variables.items():
        for column in column_names:
            if column in period_data_wide_stats.index and column not in columns_to_drop:
                period_data_wide_stats.at[column, "Category"] = category
                period_data_wide_stats.at[column, "Description"] = variable_descriptions.get(column, "No description available")

    # Group by Description, with "Other" category at the top
    grouped_stats = period_data_wide_stats.copy()
    # Add a helper column for sorting: "Other" first, then alphabetical
    grouped_stats["_sort_key"] = grouped_stats["Category"].apply(lambda x: "0" if x == "Other" else f"1_{x}")
    grouped_stats = grouped_stats.sort_values("_sort_key").drop(columns="_sort_key")
    grouped_stats.to_csv(f"{os.path.splitext(period_wide_output_file_path)[0]}_Descriptive_Stats.csv", mode='w')
    print("Summary of wide period data saved")



    # Save constants to a separate file
    with open(f"{period_output_file_path[:-4]}_CONSTANTS.txt", "w") as f:
        f.write(f"SHORTEN_DATA: {SHORTEN_DATA} \n")
        f.write(f"NUMBER_OF_ROWS_TESTING: {NUMBER_OF_ROWS_TESTING} \n")
        f.write(f"PREBIRTH_AGES_PER_CHILD: {PREBIRTH_AGES_PER_CHILD} \n")
        f.write("\n\n")
        f.write(f"SEVERAL_TIMES_PER_YEAR: {SEVERAL_TIMES_PER_YEAR} \n")
        f.write(f"SEVERAL_TIMES_PER_MONTH: {SEVERAL_TIMES_PER_MONTH} \n")
        f.write(f"MORE_THAN_ONCE_PER_DAY: {MORE_THAN_ONCE_PER_DAY} \n")
        f.write(f"WEEKS_PER_MONTH: {WEEKS_PER_MONTH} \n")
        f.write("\n\n")
        f.write("age_periods: {\n")
        for key, value in age_periods.items():
            f.write(f"{key}: {value}\n")
        f.write("} \n\n")
        f.write("columns_to_drop: [ \n")
        for column in columns_to_drop: 
            f.write(f"{column} \n")
        f.write("] \n\n")
        f.write("variable_ages: { \n")
        for key, value in variable_ages.items():
            f.write(f"{key}: {value}\n")
        f.write("} \n\n")

    

    


# End of the script

if __name__ == "__main__": 
    main()