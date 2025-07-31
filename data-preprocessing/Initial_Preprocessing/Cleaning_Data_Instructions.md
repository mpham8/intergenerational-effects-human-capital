### How to Clean the Data

This file is meant to provide clear instructions on how to go about cleaning the data. The way I (Bijan Taheri) did it is probably not the most efficient way, but it should work, provided you follow all of these steps. 

## Step 1: Setting up the Data

If you want to use data from the NLS investigator, you can start with the tagsets I have uploaded. The tagset with the child's data will be from the CNLSY79 (with a filename ending in .CHILDYA), and the tagset with the mother's data will be from the NLSY79 (with a filename ending in .NLSY79).  

# Choosing the Data: Include All Applicable Years

It's important to note that, when you're selecting the data in the NLS investigator, you'll need to select data from all applicable years. You aren't able to do this through the RNUM or QNAME fields, as these are unique to each year and question. Instead, I would recommend using the "Codebook" search feature. Sometimes, the questions slightly change in wording over time, so if you notice missing years for a question, broaden your search to include alternative phrasings. 

Once you've selected the data, you'll want to save the tagset to the server (in case you want to update it later) and use the "Basic Download" feature to download your data. 

# Modify the "-value-labels.do" file

Instrucitons for this step are also available in the Master_STATA.do file. In sum, you'll want to COMMENT all the lines that define value labels (since it interferes with exporting the data) and UNCOMMENT all the lines that rename the RNUM codes to the question codes. Use "/*" and "*/" to comment out the block of code defining value labels, and delete these comments for the block of code renaming the variables. 

## Step 2: Run "Master_STATA.do"

"Master_STATA.do" is a master file which runs the respective "-value-labels.do" functions for both the CNLSY and NLSY data. Additionally, for the CNLSY data, it runs "Rename_Names_To_Label.do," a file that transforms question names to their respective variable labels. (This is not needed for the NLSY data due to more descriptive question names.) 

Make sure to change the filepaths to the correct ones. If you're getting an error, (specifically the "type mismatch" error), it may be because you added some data and need to program a manual exception to rename it. In particular, this error will occur when two columns are assigned the same name. It's important to note that misaligned names for columns are okay. We will fix them in the next step. 

## Step 3: Run "RD_Cleaning_Data_Code.py" with the renamed data

"RD_Cleaning_Data_Code.py" serves many functions. Primarily, it serves to: 
1. Aggregate question-year data into a child-age panel
2. Aggregate the child-age panel into a child-period panel (using interpolation)
3. Save the child-age panel, the child-period panel (in both long and wide formats), descriptive statistics, and constants used in preprocessing. 

The "PATH_ENDING" variable ensures continuity when naming output files. By changing the "PATH_ENDING" variable, you will change all output files to include that ending. 

When adding new data, you may need to edit the "poorly_named_columns" dictionary, which maps different names of columns onto one representative column. This is primarily useful for different naming conventions for the same variable (e.g. "URBAN_RURAL" and "URBAN_RURAL_REVISED"). If you feel like the name given by the column is not descriptive enough, use the "better_named_columns" dictionary to map the old name to a new one. 

If you need to rescale multiple-choice questions to a frequency variable, add the data to the "rescaling_variables" dictionary, which maps column names to a list of values. The code will map the index of each element in the list to the list value. For example, "HOW_OFTEN_MOM_READS" : [50, 65, 80, 200, 500] will, in the "HOW_OFTEN_MOM_READS" column, map "1" to "50", "2" to "65", "3" to "80", "4" to "200", and "5" to "500". "rescaling_variables_by_age" works in a similar way, simply adding another field to provide the age range for the rescaling of the variable (this is only needed for variables for which the scale of the question changes by age). 

More detailed comments are available in the code for other variables to edit. 

## You're Done! 
If all goes to plan, this should spit out a child-age and child-period panel. The saved child-age panel serves as a (mostly) raw panel, including negative values (signifying non-response codes) and no interpolation. This child-age panel is filled using interpolation and constant extrapolation before being aggregated into the child-period panel. 

If you have any questions, please email btaheri1@swarthmore.edu. Happy coding! 

By Bijan Taheri, Swarthmore College '28, in conjunction with Professor Stephen O'Connell's Summer 2025 lab group. 
