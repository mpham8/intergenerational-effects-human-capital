/* 
Bijan Taheri
June 2025
Professor O'Connell, Michael Pham


This code runs all other code files. Make sure to change the data paths to line up
with where you saved them. 




NOTE: you must edit the value-labels.do file every time you download a new dataset

Instructions: 
1. OPEN the value-labels.do file in a do-file-editor (it should have the same filepath
as suggested by the "rename_question_codes_filepath" variable)
2. COMMENT all the "label define" and "label values" lines, which form the first 
section of the code (if not, this will result in saving non-numeric data)
3. UN-COMMENT all "rename" lines, which form the second section of the code

*/

* Filepaths
local nls_filepath = "06-04-10am" // REPLACE WITH YOUR DATA PATH
local rename_variable_labels_filepath = "Rename_Names_To_Labels.do" // ALSO REPLACE
local save_filepath = "06-04-11am-renamed.csv"


local dct_filepath = "`nls_filepath'/`nls_filepath'.dct"
local rename_question_codes_filepath = "`nls_filepath'/`nls_filepath'-value-labels.do"

* Loading the data
clear all
infile using "`dct_filepath'"


* Renaming to question codes using code copied from the value-labels.do file
do "`rename_question_codes_filepath'"

* Renaming to final version (modified variable labels) using custom code
do "`rename_variable_labels_filepath'"

* Save final file as a csv
export delimited "`save_filepath'"
