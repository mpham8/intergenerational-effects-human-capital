### How to Clean the Data

This file is meant to provide clear instructions on how to go about cleaning the data. The way I (Bijan Taheri) did it is probably not the most efficient way, but it (hopefully) works

## Step 1: Download/Choose the Data

If you want to use data from the NLS investigator, you can start with the tagset Bijan has uploaded. The tagset contains all variables from the CNLSY79, and will be updated whenever we decide to include (or exclude) some variables. 

# Step 1a: Modify the "-value-labels.do" file

Instrucitons for this step are also available in the Master_STATA.do file. Basically, you'll want to COMMENT all the lines that define value labels (since it messes with exporting the data as of now) and UNCOMMENT all the lines that rename the RNUM codes to the question codes. 

## Step 2: Run "Master_STATA.do"

Make sure to change the filepaths to the correct ones. If you're getting an error, (specifically the "type mismatch" error), it may be because you added some data and need to program a manual exception to rename it. 

## Step 3: Run "RD_Cleaning_Data_Code.py" with the renamed data

The cleaning data code in Python relies on the renaming in STATA, so make sure to do that step first. 

## You're Done! 
If all goes to plan, this should spit out a child-age and child-period panel. All values are replaced with "NaN" values if they don't exist in the data. 
