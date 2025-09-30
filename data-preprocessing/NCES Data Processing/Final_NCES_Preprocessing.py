import pandas as pd
import numpy as np
import os
from sklearn.linear_model import LinearRegression
import matplotlib.pyplot as plt

'''TOTALEXP (total expenditures) = TCURELSC + TNONELSE + TCAPOUT + L12 + M12 + Q11 + I86 + V91 + V92
    TCURELSC = TOTAL CURRENT EXPENDITURES FOR ELEMENTARY/SECONDARY EDUCATION
    TNONELSE = TOTAL NON-ELEMENTARY/SECONDARY EXPENDITURES
    TCAPOUT = TOTAL CAPITAL OUTLAY EXPENDITURES
    L12 = PAYMENTS TO STATE GOVERNMENTS
    M12 = PAYMENTS TO LOCAL GOVERNMENTS
    Q11 = PAYMENTS TO OTHER SCHOOL SYSTEMS
    I86 = INTEREST ON DEBT
    V91 = PAYMENTS TO PRIVATE SCHOOLS
    V92 = PAYMENTS TO CHARTER SCHOOLS
V33 = STUDENT POPULATION (ENROLLMENT)
'''

#We will only be using TCURELSC per student, calculated by aggregating all observations per county and year, and dividing by the number of students in that county and year.

############### Concatenate Unprocessed Data Files ###############

# Load CPI Data
cpi_data = pd.read_csv("C:/Users/kahna/Dropbox/OConnell 2025 Research/CPI Data/Processed_CPI_Data.csv")

# Load NCES data files for each year
year_range = range(1995, 2023)
all_years = []

for year in year_range:
    year_suffix = str(year)[-2:]
    file_path = f'C:/Users/kahna/Dropbox/OConnell 2025 Research/NCES Data Processing/Raw Data/sdf{year_suffix}.txt'
    if os.path.exists(file_path):
        df = pd.read_csv(file_path, sep='\t', low_memory=False, encoding='latin1')
        df['Year'] = year

        # Create FIPS column from FIPST + FIPSCO (combining state and county FIPS codes) for files where CONUM is missing
        if 'CONUM' not in df.columns or df['CONUM'].isnull().all():
           df['CONUM'] = df['FIPST'].fillna(0).astype(int).astype(str).str.zfill(2) + df['FIPSCO'].fillna(0).astype(int).astype(str).str.zfill(3)

        # Subtract V91 (payments to private schools) and V92 (payments to charter schools) from TCURELSC
        # In accordance with NCES documentation, these values shouldn't be included in per pupil spending calculations
        if 'V91' in df.columns:
            df['V91'] = pd.to_numeric(df['V91'], errors='coerce')
            mask_v91 = (df['V91'] > 0) & (df['V91'].notna())
            df.loc[mask_v91, 'TCURELSC'] = df.loc[mask_v91, 'TCURELSC'] - df.loc[mask_v91, 'V91']
            
        if 'V92' in df.columns:
            df['V92'] = pd.to_numeric(df['V92'], errors='coerce')
            mask_v92 = (df['V92'] > 0) & (df['V92'].notna())
            df.loc[mask_v92, 'TCURELSC'] = df.loc[mask_v92, 'TCURELSC'] - df.loc[mask_v92, 'V92']

        cols = ['CONUM', 'Year', 'TCURELSC', 'V33']
        data = df[cols].copy()
        # Remove negative, zero, and NaN values in TCURELSC
        data['TCURELSC'] = pd.to_numeric(data['TCURELSC'], errors='coerce')
        data = data[(data['TCURELSC'] > 0) & (data['TCURELSC'].notna())]

        all_years.append(data)

# Combine all years
combined = pd.concat(all_years, ignore_index=True)

# Rename columns
combined.rename(columns= {'CONUM': 'FIPS', 'V33': 'student_pop'}, inplace=True)

# Pad all FIPS codes to ensure they are 5 digits and remove codes beginning in M
combined['FIPS'] = pd.to_numeric(combined['FIPS'], errors='coerce')
combined = combined[combined['FIPS'].notna()]
combined['FIPS'] = combined['FIPS'].astype(int).astype(str).str.zfill(5)


############### Aggregate data by FIPS and year ###############

# Sum TCURELSC and student_pop for each FIPS-year combination
aggregated = combined.groupby(['FIPS', 'Year'], as_index=False).agg({
    'TCURELSC': 'sum',
    'student_pop': 'sum'
}).reset_index(drop=True)

# Create TCURELSC per student column
aggregated['TCURELSC_per_student'] = aggregated['TCURELSC'] / aggregated['student_pop']
aggregated = aggregated[(aggregated['TCURELSC_per_student'] > 0) & (aggregated['student_pop'] > 0)]


############# Deflate TCURELSC per student using CPI data #############

# Dictionary to map FIPS codes to regions
fips_to_region = {
    'Northeast': ['09', '23', '25', '33', '34', '36', '42', '44', '50'],
    'Midwest': ['17', '18', '19', '20', '26', '27', '29', '31', '38','39',
                '46','55'],
    'South': ['01', '05', '10', '11', '12', '13', '21', '22', '24', '28',
                '37', '40', '45', '47', '48', '51', '54'],
    'West': ['02', '04', '06', '08', '15', '16', '30', '32', '35', '41',
                '49', '53', '56']
}

# Function to get region from FIPS code
def get_region(fips):
    for region, fips_list in fips_to_region.items():
        if fips[:2] in fips_list:
            return region
    return None

# Add region column to the aggregated data
aggregated['Region'] = aggregated['FIPS'].apply(get_region)

# Merge with CPI data
real_data = pd.merge(aggregated, cpi_data, on=['Year', 'Region'], how='left')

# Inflate TCURELSC per student using CPI data with 2024 as the base year
real_data['real_TCURELSC_per_student'] = 313.7 * real_data['TCURELSC_per_student'] / real_data['Annual_CPI']


############### Extrapolate Backwards for Years 1990-1995 ###############

"""
The extrapolation is done using growth rates from 1990-1995 of official NCES national expenditures per student data (2002 dollars)
Data from later years is also used for FIPS values in which there is no value for 1995 and 1996
1990: $6,516
1991: $6,522
1992: $6,476
1993: $6,450
1994: $6,492
1995: $6,550
1996: $6,561
1997: $6,642
"""
# Define the growth rates (negative means decrease from previous year)
growth_rates = {
    1996: 0.0123,  # 1.23%
    1995: 0.0017,  # 0.17%
    1994: 0.0089,  # 0.89%
    1993: 0.0065,  # 0.65%
    1992: -0.0040, # -0.40%
    1991: -0.0073, # -0.73%
    1990: -0.0009  # -0.09%
}

#Create complete index with all FIPS-year combinations
all_fips = real_data['FIPS'].unique()
all_years = range(1990, 2023)

# Create all possible combinations using pandas
fips_df = pd.DataFrame({'FIPS': all_fips, 'key': 1})
years_df = pd.DataFrame({'Year': all_years, 'key': 1})
complete_df = pd.merge(fips_df, years_df, on='key').drop('key', axis=1)

# Merge to fill in existing data
complete_data = complete_df.merge(real_data, on=['FIPS', 'Year'], how='left')

# Count missing years per FIPS code
missing_by_fips = complete_data.groupby('FIPS')['TCURELSC_per_student'].apply(
    lambda x: x.isna().sum()
).reset_index(name = 'Missing_Years')

# Filter complete_data to only include FIPS codes with 10 or fewer missing years
filtered_fips = missing_by_fips[missing_by_fips['Missing_Years'] <= 10]['FIPS']
complete_data = complete_data[complete_data['FIPS'].isin(filtered_fips)]
complete_data = complete_data.sort_values(['FIPS', 'Year'])


extrapolated_data = complete_data.copy()

# Perform backwards extrapolation for each FIPS group separately
extrapolated_list = []

for fips in extrapolated_data['FIPS'].unique():
    fips_data = extrapolated_data[extrapolated_data['FIPS'] == fips].copy()
    
    # Work backwards from 1996 to 1990
    for year in range(1996, 1989, -1):  # 1996, 1995, 1994, 1993, 1992, 1991, 1990
        current_row = fips_data[fips_data['Year'] == year]
        next_year_row = fips_data[fips_data['Year'] == year + 1]
        
        # Check if current year is missing and next year exists
        if (pd.isna(current_row['real_TCURELSC_per_student'].iloc[0]) and
            pd.notna(next_year_row['real_TCURELSC_per_student'].iloc[0])):
            
            # Calculate the value using backwards growth rate
            next_year_value = next_year_row['real_TCURELSC_per_student'].iloc[0]
            growth_rate = growth_rates[year]
            
            current_year_value = next_year_value / (1 + growth_rate)
            
            # Update the value
            fips_data.loc[fips_data['Year'] == year, 'real_TCURELSC_per_student'] = current_year_value
            
            print(f"FIPS {fips}, Year {year}: Extrapolated {current_year_value:.2f} from {year+1} value {next_year_value:.2f} using growth rate {growth_rate:.4f}")
    
    extrapolated_list.append(fips_data)

# Combine all FIPS data back together
extrapolated_data = pd.concat(extrapolated_list, ignore_index=True)


############### Remove FIPS Values with more than 10 years of being an outlier ###############

# Calculate mean and standard deviation for real_TCURELSC_per_student for each year
yearly_stats = extrapolated_data.groupby('Year')['real_TCURELSC_per_student'].agg(['mean', 'std']).reset_index()
yearly_stats.columns = ['Year', 'mean', 'std']

# Create a dictionary for faster lookup
yearly_stats_dict = {
    int(row['Year']): {
        'mean': float(row['mean']),
        'std': float(row['std'])
    }
    for _, row in yearly_stats.iterrows()
}

# Function to check if value is an outlier (3+ standard deviations away)
def is_outlier(value, year_stats):
    if pd.isna(value):
        return False
    z_score = abs(value - year_stats['mean']) / year_stats['std']
    return z_score >= 3

# Count outlier years for each FIPS
fips_outlier_counts = {}

for fips in extrapolated_data['FIPS'].unique():
    fips_data = extrapolated_data[extrapolated_data['FIPS'] == fips]
    outlier_count = 0
    
    for _, row in fips_data.iterrows():
        year = int(row['Year'])
        value = row['real_TCURELSC_per_student']
        
        if year in yearly_stats_dict and pd.notna(value):
            if is_outlier(value, yearly_stats_dict[year]):
                outlier_count += 1
    
    fips_outlier_counts[fips] = outlier_count

# Identify FIPS codes with 10 or more outlier years
outlier_fips = [fips for fips, count in fips_outlier_counts.items() if count >= 10]
print("The list of outliers:", outlier_fips)

# Remove FIPS codes with 10 or more outlier years that aren't one of the 100 wealthiest counties in the U.S.
rich_counties = {'06003', '08053', '34003', '34013', '36059',
                 '36079', '36087', '36103', '48033', '48269',
                 '51013'}
filtered_data = extrapolated_data[
    (~extrapolated_data['FIPS'].isin(outlier_fips)) | 
    (extrapolated_data['FIPS'].isin(rich_counties))
]


print(f"Removed {len(outlier_fips)} FIPS codes for being outliers")

# Create a histogram showing distribution of outlier years
outlier_counts = list(fips_outlier_counts.values())

plt.figure(figsize=(12, 6))
plt.hist(outlier_counts, bins=range(1, max(outlier_counts) + 2), alpha=0.7, 
         color='skyblue', edgecolor='black')
plt.axvline(x=10, color='red', linestyle='--', linewidth=2, 
           label='Removal threshold (10 years)')
plt.xlabel('Number of Outlier Years per FIPS')
plt.ylabel('Number of FIPS Codes')
plt.title('Distribution of Outlier Years per FIPS Code')
plt.legend()
plt.grid(axis='y', alpha=0.3)
plt.show()

# Update the final_data to use filtered data
final_data = filtered_data[['FIPS', 'Year', 'student_pop', 'real_TCURELSC_per_student']]



############### Save Data ###############

#Plot complete_data vs extrapolated_data for five random values of FIPS
random_fips = np.random.choice(extrapolated_data['FIPS'].unique(), 5, replace=False)

for fips in random_fips:
    fips_data = complete_data[complete_data['FIPS'] == fips]
    fips_extrapolated = extrapolated_data[extrapolated_data['FIPS'] == fips]
    
    plt.plot(fips_data['Year'], fips_data['real_TCURELSC_per_student'], marker='o', label=f'Original {fips}')
    plt.plot(fips_extrapolated['Year'], fips_extrapolated['real_TCURELSC_per_student'], marker='x', linestyle='--', label=f'Extrapolated {fips}')

    plt.title('Original Data vs Extrapolated Data')
    plt.xlabel('Year')
    plt.ylabel('TCURELSC per Student')
    plt.legend()
    plt.show()


final_data = extrapolated_data[['FIPS', 'Year', 'student_pop','real_TCURELSC_per_student']]


# Save descriptive statistics by year
descriptive_stats = final_data.describe()
descriptive_stats.to_csv('Final_NCES_Data_Descriptive_Statistics.csv')

final_data.to_csv('Final_Extrapolated_NCES_Data.csv', index=False)


# Save descriptive statistics by year
yearly_stats = final_data.groupby('Year')[['student_pop', 'real_TCURELSC_per_student']].describe()

# Flatten the multi-level column index for easier reading
yearly_stats.columns = ['_'.join(col).strip() for col in yearly_stats.columns]

# Save to CSV
yearly_stats.to_csv('Final_NCES_Data_Yearly_Descriptive_Statistics.csv')