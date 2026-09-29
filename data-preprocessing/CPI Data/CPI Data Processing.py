import pandas as pd

# Concatenates annual CPI data from different regions into a single DataFrame and saves it as a CSV file.
# 1982 is used as the base year


Midwestcpi = pd.read_excel("C:/Users/kahna/Dropbox/OConnell 2025 Research/CPI Data/Midwest_CPI.xlsx", skiprows=11)
Northeastcpi = pd.read_excel("C:/Users/kahna/Dropbox/OConnell 2025 Research/CPI Data/Northeast_CPI.xlsx", skiprows=11)
Southcpi = pd.read_excel("C:/Users/kahna/Dropbox/OConnell 2025 Research/CPI Data/South_CPI.xlsx", skiprows=11)
Westcpi = pd.read_excel("C:/Users/kahna/Dropbox/OConnell 2025 Research/CPI Data/West_CPI.xlsx", skiprows=11)

#Concatenate CPI data from all regions
Midwestcpi['Region'] = 'Midwest'
Northeastcpi['Region'] = 'Northeast'
Southcpi['Region'] = 'South'
Westcpi['Region'] = 'West'
all_cpi = pd.concat([Midwestcpi, Northeastcpi, Southcpi, Westcpi], ignore_index=True)

all_cpi.rename(columns={'Annual': 'Annual_CPI'}, inplace=True)
all_cpi = all_cpi[['Year', 'Region', 'Annual_CPI']]

all_cpi.to_csv("C:/Users/kahna/Dropbox/OConnell 2025 Research/CPI Data/Processed_CPI_Data.csv", index=False)

