import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from linearmodels.panel import PanelOLS

cpi_data = pd.read_csv("C:/Users/kahna/Dropbox/OConnell 2025 Research/CPI Data/Processed_CPI_Data.csv")
prices = pd.read_excel("C:/Users/kahna/Dropbox/OConnell 2025 Research/Childcare Prices Data Processing/NDCP_Raw_Data.xlsx")

############## Price Data Preparation ###############

#Weighted prices calculation for center and family care, based on length of periods of infant, toddler, and preschool care
prices['Weighted_Center_Price'] = (prices['MCINFANT']*24 + prices['MCTODDLER']*12 + prices['MCPRESCHOOL']*18) / 54
prices['Weighted_Family_Care_Price'] = (prices['MFCCINFANT']*24 + prices['MFCCTODDLER']*12 + prices['MFCCPRESCHOOL']*18) / 54

# Rename columns for clarity
prices.rename(columns={
    'COUNTY_FIPS_CODE': 'FIPS', 
    'STUDYYEAR': 'Year'
}, inplace=True)

prices = prices[['FIPS', 'Year', 'Weighted_Center_Price', 'Weighted_Family_Care_Price']]

prices['FIPS'] = prices['FIPS'].astype(float).astype(int).astype(str).str.zfill(5)


############## Merge with CPI Data ###############

#Dictionary of FIPS codes for each region
fips_to_region = {
    'Northeast': ['09', '23', '25', '33', '34', '36', '42', '44', '50'],
    'Midwest': ['17', '18', '19', '20', '26', '27', '29', '31', '38','39',
                '46','55'],
    'South': ['01', '05', '10', '11', '12', '13', '21', '22', '24', '28',
                '37', '40', '45', '47', '48', '51', '54'],
    'West': ['02', '04', '06', '08', '15', '16', '30', '32', '35', '41',
                '49', '53', '56']
}

#Returns the region based on the FIPS code.
def get_region(fips):
    for region, fips_list in fips_to_region.items():
        if fips[:2] in fips_list:
            return region
    return None

prices['Region'] = prices['FIPS'].apply(get_region)

merged_data = pd.merge(prices, cpi_data, on=['Year', 'Region'], how='left')


############## Compute Regression and Extrapolate Prices for Years 1990-2007 ###############

# Create all combinations of FIPS and years to later fill with predicted values
fipslist = prices[['FIPS', 'Region']].drop_duplicates()
years = pd.DataFrame({'Year': range(1990, 2023)})
fips_year_combos = fipslist.merge(years, how='cross')

prediction_df = pd.merge(fips_year_combos, cpi_data, on=['Region', 'Year'], how='left')


# Prepares data for regression of the weighted center price
logcenterdata = merged_data.copy()
logcenterdata['Log_Weighted_Center_Price'] = np.log(logcenterdata['Weighted_Center_Price'])
logcenterdata['Log_Annual_CPI'] = np.log(logcenterdata['Annual_CPI'])
logcenterdata = logcenterdata.set_index(['FIPS', 'Year'])
logcenterdata = logcenterdata[['Log_Weighted_Center_Price', 'Log_Annual_CPI']].dropna()

#Compute fixed effects log-log regression for the weighted center price
logcentermodel = PanelOLS.from_formula('Log_Weighted_Center_Price ~ Log_Annual_CPI + EntityEffects', data=logcenterdata)
logcenterresults = logcentermodel.fit()
print(logcenterresults.summary)

# Save results and use them to predict prices for all FIPS and years
beta = logcenterresults.params['Log_Annual_CPI']
mu_i = logcenterresults.estimated_effects.unstack().mean(axis=1).to_dict()
prediction_df['Predicted_Center_Price'] = np.exp(prediction_df['FIPS'].map(mu_i) + beta * np.log(prediction_df['Annual_CPI']))


# Repeat for regression of the weighted family care price
logfamilydata = merged_data.copy()
logfamilydata['Log_Weighted_Family_Care_Price'] = np.log(logfamilydata['Weighted_Family_Care_Price'])
logfamilydata['Log_Annual_CPI'] = np.log(logfamilydata['Annual_CPI'])
logfamilydata = logfamilydata.set_index(['FIPS', 'Year'])
logfamilydata = logfamilydata[['Log_Weighted_Family_Care_Price', 'Log_Annual_CPI']].dropna()

logfamilymodel = PanelOLS.from_formula('Log_Weighted_Family_Care_Price ~ Log_Annual_CPI + EntityEffects', data=logfamilydata)
logfamilyresults = logfamilymodel.fit()
print(logfamilyresults.summary)

beta = logfamilyresults.params['Log_Annual_CPI']
mu_i = logfamilyresults.estimated_effects.unstack().mean(axis=1).to_dict()
prediction_df['Predicted_Family_Care_Price'] = np.exp(prediction_df['FIPS'].map(mu_i) + beta * np.log(prediction_df['Annual_CPI']))


# Add the predicted prices to the original prices DataFrame for missing years
extrapolated = pd.merge(prices, prediction_df, on=['FIPS', 'Year'], how='right')

extrapolated['Weighted_Center_Price'] = extrapolated['Weighted_Center_Price'].fillna(extrapolated['Predicted_Center_Price'])
extrapolated['Weighted_Family_Care_Price'] = extrapolated['Weighted_Family_Care_Price'].fillna(extrapolated['Predicted_Family_Care_Price'])


# Convert prices to real prices using the CPI with base year 1982
extrapolated['Real_Weighted_Center_Price'] = extrapolated['Weighted_Center_Price'] * 100 / extrapolated['Annual_CPI']
extrapolated['Real_Weighted_Family_Care_Price'] = extrapolated['Weighted_Family_Care_Price'] * 100 / extrapolated['Annual_CPI']


extrapolated = extrapolated[['FIPS', 'Year', 'Real_Weighted_Center_Price', 'Real_Weighted_Family_Care_Price']]

extrapolated.to_csv('Final_Prices_With_Extrapolation.csv', index=False)