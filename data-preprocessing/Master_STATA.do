/* 
Bijan Taheri
June 2025
Professor O'Connell, Michael Pham


This code runs all other code files. Make sure to change the data paths to line up
with where you saved them. 

It is meant to run in the data-preprocessing directory of the project. 


NOTE: you must edit the value-labels.do file every time you download a new dataset

Instructions: 
1. OPEN the value-labels.do file in a do-file-editor (it should have the same filepath
as suggested by the "rename_question_codes_filepath" variable)
2. COMMENT all the "label define" and "label values" lines, which form the first 
section of the code (if not, this will result in saving non-numeric data)
3. UN-COMMENT all "rename" lines, which form the second section of the code

*/

* Filepaths
local cnls_filepath = "06-04-10am" // REPLACE WITH YOUR DATA PATH
local nls79_filepath = "06-04-mother-data-simplified"
local rename_variable_labels_filepath = "Rename_Names_To_Labels.do" // ALSO REPLACE
local cnls_save_filepath = "06-05-7pm-renamed.csv"
local nls79_save_filepath = "06-05-mother-simple-renamed.csv"


local cnls_dct_filepath = "`cnls_filepath'/`cnls_filepath'.dct"
local cnls_rename_qcodes_filepath = "`cnls_filepath'/`cnls_filepath'-value-labels.do"
local nls79_dct_filepath = "`nls79_filepath'/`nls79_filepath'.dct"
local nls79_rename_qcodes_filepath = "`nls79_filepath'/`nls79_filepath'-value-labels.do"

* Loading the data
clear all
infile using "`cnls_dct_filepath'"

* * FOR CNLS DATA
* Renaming to question codes using code copied from the value-labels.do file
do "`cnls_rename_qcodes_filepath'"
* Renaming to final version (modified variable labels) using custom code
do "`rename_variable_labels_filepath'"


* Save file as a csv
export delimited "`cnls_save_filepath'"

* Start over 
* clear all


* infile using "`nls79_dct_filepath'"
* * FOR NLS79 DATA
* Renaming to question codes
* do "`nls79_rename_qcodes_filepath'"
* Since question codes are much better for this dataset, we can simply save
* export delimited "`nls79_save_filepath'"

