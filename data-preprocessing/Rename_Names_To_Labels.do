
/* 
Bijan Taheri
June 2025
Professor O'Connell, Michael Pham
STATA 19


This code is meant to rename the question codes to variants of their variable labels, adding the date. 


*/

/* 
Special cases (duplicates)

HOME B (3-5): MOM HELPS CH LEARN ______
TOTAL FAMILY INCOME FROM ALL SOURCES _______
TYPE OF SCHOOL CHILD ATTENDS ______

*/ 
quietly describe, varlist
local all_vars `r(varlist)'

local counter = 0 // for debugging purposes only

foreach var of local all_vars {
    local lab : variable label `var'
    if "`lab'" == "" || "`lab'" == " " {
		error("No variable label.")
	}
	* Extract last 4 digits from variable name (assumed to be the date)
	local varname = "`var'"
	local varlen = length("`varname'")
	local date_suffix = substr("`varname'", `varlen'-3, 4)
	
	
	* Check if last 4 characters are actually digits
	local is_numeric = 1
	forvalues i = 1/4 {
		local char = substr("`date_suffix'", `i', 1)
		if "`char'" < "0" | "`char'" > "9" {
			local is_numeric = 0
		}
	}
	
	* If last 4 chars aren't numeric, skip
	if `is_numeric' == 0 {
		continue
	}
	
	
	* If the last two (or four) digits of the variable label are numeric, don't 
	* include them in the clean variable label
	local is_numeric_4 = 1
	local is_numeric_2 = 1
	local lablen = length("`lab'")
	forvalues i = 1/4 {
		local char = substr("`lab'", `lablen'-`i'+1, 1)
		if "`char'" < "0" | "`char'" > "9" {
			local is_numeric_4 = 0
		}
	}
	
	
	forvalues i = 1/2 {
		local char = substr("`lab'", `lablen'-`i'+1, 1)
		if "`char'" < "0" | "`char'" > "9" {
			local is_numeric_2 = 0
		}
	}
	
	if `is_numeric_4' == 1 {
		local clean_label = substr("`lab'", 1, `lablen' - 4)
	}
	else if `is_numeric_2' == 1 {
		local clean_label = substr("`lab'", 1, `lablen' - 2)
	}
	else {
		local clean_label = "`lab'"
	}
	
	
	
	* Clean the label to make it a valid variable name
	
	* If the label has "Home" or "Check" in front, remove it (to give more room for 
	* actual description of the filenames)
	
// 	if substr("`lab'", 1, 4) == "HOME" {
// 		local clean_label = substr("`lab'", 4, .)
// 	}
// 	else if substr("`lab'", 1, 5) == "CHECK" {
// 		local clean_label = substr("`lab'", 5, .)
// 	}
// 	else if substr("`lab'", 1, 5) == "CHILD" {
// 		local clean_label = substr("`lab'", 5, .)
// 	}

	* Replace spaces and special characters with underscores
	local clean_label = usubinstr("`clean_label'", " ", "_", .)
	local clean_label = usubinstr("`clean_label'", "-", "_", .)
	local clean_label = usubinstr("`clean_label'", "(", "_", .)
	local clean_label = usubinstr("`clean_label'", ")", "_", .)
	local clean_label = usubinstr("`clean_label'", "/", "_", .)
	local clean_label = usubinstr("`clean_label'", ".", "_", .)
	local clean_label = usubinstr("`clean_label'", ",", "_", .)
	local clean_label = usubinstr("`clean_label'", ":", "_", .)
	local clean_label = usubinstr("`clean_label'", ";", "_", .)
	local clean_label = usubinstr("`clean_label'", "?", "_", .)
	local clean_label = usubinstr("`clean_label'", "!", "_", .)
	local clean_label = usubinstr("`clean_label'", "@", "_", .)
	local clean_label = usubinstr("`clean_label'", "#", "_", .)
	local clean_label = usubinstr("`clean_label'", "$", "_", .)
	local clean_label = usubinstr("`clean_label'", "%", "_", .)
	local clean_label = usubinstr("`clean_label'", "^", "_", .)
	local clean_label = usubinstr("`clean_label'", "&", "_", .)
	local clean_label = usubinstr("`clean_label'", "*", "_", .)
	local clean_label = usubinstr("`clean_label'", "+", "_", .)
	local clean_label = usubinstr("`clean_label'", "=", "_", .)
	local clean_label = usubinstr("`clean_label'", "|", "_", .)
	local clean_label = usubinstr("`clean_label'", "\", "_", .)
	local clean_label = usubinstr("`clean_label'", "'", "_", .)
	local clean_label = usubinstr("`clean_label'", `"""', "_", .)
	local clean_label = usubinstr("`clean_label'", "<", "_", .)
	local clean_label = usubinstr("`clean_label'", ">", "_", .)
	
	
	* Remove multiple consecutive underscores
	while strpos("`clean_label'", "__") > 0 {
		local clean_label = usubinstr("`clean_label'", "__", "_", .)
	}
	
	* Shorten it to fit in 32-character limit (includeing "_" and date suffix)
	if length("`clean_label'") > 27 {
		local clean_label = substr("`clean_label'", 1, 27)
	}
	
	
	* Remove leading/trailing underscores
	if substr("`clean_label'", 1, 1) == "_" {
		local clean_label = substr("`clean_label'", 2, .)
	}
	if substr("`clean_label'", -1, 1) == "_" {
		local clean_label = substr("`clean_label'", 1, length("`clean_label'")-1)
	}
	
	
	* Combine cleaned label with date suffix
	local newname = "`clean_label'_`date_suffix'"
	
	
	
	di "Working to rename column to `newname'"
	
	* Check if new name already exists. If so, it's a specific exception
	capture confirm variable `newname'
	if !_rc {
		if substr("`lab'", 1, 4) == "HOME" {
			local clean_label = substr("`lab'", 15, .)
		}
		else if substr("`lab'", 1, 5) == "TOTAL" {
			local clean_label = substr("`lab'", 7, .)
		}
		else if substr("`lab'", 1, 4) == "TYPE" {
			local clean_label = substr("`lab'", 9, .)
		}
		else {
			error("Unexpected variable name duplicate. Add it to exceptions")
		}
		local clean_label = usubinstr("`clean_label'", " ", "_", .)
		local clean_label = usubinstr("`clean_label'", ":", "_", .)
		local clean_label = usubinstr("`clean_label'", "(", "_", .)
		local clean_label = usubinstr("`clean_label'", ")", "_", .)
		
		if length("`clean_label'") > 27 {
			local clean_label = substr("`clean_label'", 1, 27)
		}
		while strpos("`clean_label'", "__") > 0 {
			local clean_label = usubinstr("`clean_label'", "__", "_", .)
		}
		local newname = "`clean_label'_`date_suffix'"
		
	}
	di "Working to rename column to `newname'"
	
	* Rename the variable (using capture to handle any remaining issues)
	capture rename `var' `newname'
	if _rc {
		error("Ran into problems combining variable names. ")
	}
	else {
		display "Successfully renamed `var' (date: `date_suffix') to `newname'"
	}
}
