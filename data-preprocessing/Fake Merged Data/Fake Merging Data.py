import pandas as pd 
import numpy as np

child = pd.read_csv('child_period_panel_BEST.csv')
child = child[child['period'] >= 0]  # Only keep data from periods 0-4
child = child.dropna(subset=['CYRB_XRND']) #Get rid of data where birthyear is 0

ginvestment = pd.read_csv('NCES Data Processing/Final_Interpolated_NCES_Data.csv')
prices = pd.read_csv('Childcare Prices Data Processing/Final_Prices_With_Extrapolation.csv')


# Generate random FIPS codes for the child DataFrame from ginvestment and prices DataFrames
fips2 = ginvestment['FIPS'].values
fips3 = prices['FIPS'].values

choices = np.random.rand(len(child)) < 0.5  # True for df2, False for df3

child['FIPS'] = np.where(
    choices,
    np.random.choice(fips2, size=len(child), replace=True),
    np.random.choice(fips3, size=len(child), replace=True)
)


# Convert FIPS to int and then to zero-padded 5-digit string to ensure proper merging
child['FIPS'] = child['FIPS'].astype(int).astype(str).str.zfill(5)
ginvestment['FIPS'] = ginvestment['FIPS'].astype(int).astype(str).str.zfill(5)
prices['FIPS'] = prices['FIPS'].astype(int).astype(str).str.zfill(5)

# Age periods dictionary
age_periods = {
    0: (0, 6), # Pre-elementary
    1: (6, 10), # Elementary
    2: (10, 15), # Secondary
    3: (15, 20) # High school
}

# Generic function to get average values for prices and ginvestment over a period
def get_average_values(fips_code, birth_year, period, lookup_dict):
    start_age, end_age = age_periods[period]
    
    # Convert birth_year to int to handle float values
    birth_year = int(birth_year)

    years_to_average = list(range(birth_year + start_age, birth_year + end_age))
    
    values = []
    for year in years_to_average:
        value = lookup_dict.get((fips_code, year), np.nan)
        if not np.isnan(value):
            values.append(value)
    
    return np.mean(values) if values else np.nan

# Create lookup dictionaries
ginvestment_lookup = ginvestment.set_index(['FIPS', 'Year'])['real_TCURELSC_per_student'].to_dict()
prices_lookup = prices.set_index(['FIPS', 'Year'])['Real_Weighted_Center_Price'].to_dict()
family_prices_lookup = prices.set_index(['FIPS', 'Year'])['Real_Weighted_Family_Care_Price'].to_dict()

# Create all columns using the generic function
child['TCURELSC_per_student'] = child.apply(
    lambda row: get_average_values(row['FIPS'], row['CYRB_XRND'], row['period'], ginvestment_lookup), axis=1
)

child['Real_Weighted_Center_Price'] = child.apply(
    lambda row: get_average_values(row['FIPS'], row['CYRB_XRND'], row['period'], prices_lookup), axis=1
)

child['Real_Weighted_Family_Care_Price'] = child.apply(
    lambda row: get_average_values(row['FIPS'], row['CYRB_XRND'], row['period'], family_prices_lookup), axis=1
)

#Makes NaN values for length in head start program 0 if there is a 0 answer for if child was in head start
child.loc[(child['HOW_LONG_CHILD_WAS_IN_HEAD'].isna()) & (child['CHILD_EVER_ENROLLED_IN_HEAD'] == 0), 'HOW_LONG_CHILD_WAS_IN_HEAD'] = 0

child.rename(columns={
    'Real_Weighted_Center_Price': 'Center_Care_Price',
    'Real_Weighted_Family_Care_Price': 'Family_Care_Price'
}, inplace=True)

# Make every NaN value 0 in MOM_HELPS_CH_LEARN_NUMBERS column
child.loc[child['MOM_HELPS_CH_LEARN_NUMBERS'].isna(), 'MOM_HELPS_CH_LEARN_NUMBERS'] = 0
child.loc[child['MOM_HELPS_CH_LEARN_ALPHABET'].isna(), 'MOM_HELPS_CH_LEARN_ALPHABET'] = 0
child.loc[child['MOM_HELPS_CH_LEARN_COLORS'].isna(), 'MOM_HELPS_CH_LEARN_COLORS'] = 0
child.loc[child['MOM_HELPS_CH_LEARN_SHAPES'].isna(), 'MOM_HELPS_CH_LEARN_SHAPES'] = 0


child.to_csv('Fake_Merged_Data.csv', index=False)

