/* 
Bijan Taheri
O'Connell Research Lab
Spring 2026


This code is meant to run some preliminary regressions to inform our simulated
method of moments, specifically calculating wage and government input trajectories

*/

* IMPORT
import delimited "data-preprocessing/Processed_Data/child_period_panel_BEST.csv", clear
keep if period >= 0
keep if period != 3

****************************************************
* RENAMING VARIABLES
****************************************************
rename tnfi_trunc income
rename hgc_rev_mom edu
rename mother_age age


****************************************************
* SET PANEL STRUCTURE
****************************************************
xtset id period



****************************************************
* 1) AR(1) PROCESS FOR INCOME
****************************************************
gen L_income = L.income
reg income L_income

****************************************************
* 2–3) RECOVER WAGE PROCESS FROM INCOME MOMENTS
****************************************************

* Log income
replace income = 0.001 if income == 0
gen ln_income = ln(income)

* Lags
gen L1_ln_income = L.ln_income
gen L2_ln_income = L2.ln_income

* Variance V
summ ln_income
scalar V = r(Var)

* Covariance C1 = Cov(y_t, y_{t-1})
corr ln_income L1_ln_income, covariance
matrix C = r(C)
scalar C1 = C[1,2]

* Covariance C2 = Cov(y_t, y_{t-2})
corr ln_income L2_ln_income, covariance
matrix C = r(C)
scalar C2 = C[1,2]

display V
display C1
display C2
display V - C1

* Parameter recovery
scalar beta = (C1 - C2) / (V - C1)
scalar sigma_w2 = (V - C1) / (1 - beta)
scalar sigma_h2 = V - sigma_w2
scalar Sigma2 = sigma_w2 * (1 - beta^2)

display "==== WITHOUT DEMEANING ===="
display "beta = " beta
display "sigma_w^2 = " sigma_w2
display "sigma_h^2 = " sigma_h2
display "Sigma^2 = " Sigma2

****************************************************
* 4) DEMEANED AR(1) (CONTROLS: AGE, EDUCATION, YEAR)
****************************************************

* Regression with controls
reg ln_income age c.age#c.age edu i.period

* Residuals
predict r, resid

* Lags of residuals
gen L1_r = L.r
gen L2_r = L2.r

* Variance
summ r
scalar V_r = r(Var)

* Covariance C1
corr r L1_r, covariance
matrix C = r(C)
scalar C1_r = C[1,2]

* Covariance C2
corr r L2_r, covariance
matrix C = r(C)
scalar C2_r = C[1,2]

* Parameter recovery
scalar beta_r = (C1_r - C2_r) / (V_r - C1_r)
scalar sigma_w2_r = (V_r - C1_r) / (1 - beta_r)
scalar sigma_h2_r = V_r - sigma_w2_r
scalar Sigma2_r = sigma_w2_r * (1 - beta_r^2)

display "==== WITH DEMEANING ===="
display "beta = " beta_r
display "sigma_w^2 = " sigma_w2_r
display "sigma_h^2 = " sigma_h2_r
display "Sigma^2 = " Sigma2_r
