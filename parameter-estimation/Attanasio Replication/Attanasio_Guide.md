# Guide to the Attanasio Estimation

##  What this code does

We use the Attanasio code to estimate the **true human capital production
function**. The basic idea is simple:

1. Child human capital and parental investment are not observed perfectly.
2. We therefore estimate them as latent factors using several observed
   measures.
3. We draw synthetic observations from the estimated factor distribution.
4. We use those observations to estimate the true human capital production
   function for each of the three childhood periods.

The final output is a bridged,  three-row file called
`attanasio_rev3_bridge.csv`. The most recent DelBoca code reads this pre-bridged file of the estimated
production-function parameters.

##  The files that are actually run

The entrypoint is:

`Master File.r`

This file calls the other five required source files. They should remain in
the same `Attanasio Replication` folder.

| File | What it does |
|---|---|
| `Master File.r` | Sets the paths and bootstrap controls, loads the required R packages, and runs the remaining files in order. |
| `OurR_MeasurementSpec_No4.r` | Loads the data, constructs the observed measures, and states which measures identify each latent factor. |
| `Our_Procedures.R` | Contains the function used to draw synthetic latent factors. |
| `Our_estim_meas_model.R` | Estimates the latent-factor measurement system. |
| `Our_Estimate_FactorModel.R` | Runs the factor estimation, saves its parameters, and estimates bootstrap versions of the factor model. |
| `Our_Production_Functions_No4_Incomeinst.r` | Estimates the nested-CES production functions and writes the bridge used by Rev3. |

The output folders are also not source code. They can be empty before a run;
the program creates the requested output directory.

##  Before running the code

###  Required software

The program requires R and the following R packages:

```r
install.packages(c(
  "R.utils",
  "ks",
  "gdata",
  "corpcor",
  "minpack.lm",
  "Matrix",
  "foreign",
  "scales",
  "openxlsx"
))
```

###  Required data

The current default input file is:

```text
'''CHANGE THIS ONCE IN THE VDE```

The data must contain observations for periods `0`, `1`, and `2`. It must also
contain the child test scores, parental-investment measures, household
characteristics, public inputs, prices, labor income, and CEX variables named
in `OurR_MeasurementSpec_No4.r`.

###  The two cardinal measures

PIAT Math is the cardinal measure of child human capital. Its factor loading is
fixed to one in all three periods.

CEX education expenditure is the cardinal measure of parental investment. The
code:

##  Running the code as it currently stands

Open a terminal and move to the `parameter-estimation` folder:

Then run:

```bash
Rscript "Attanasio Replication/Master File.r"
```


With no additional settings, the program:

- Uses the current Mac input path shown above.
- Uses all available observations.
- Runs the bootstrap.
- Uses two bootstrap samples.
- Writes the main results to:

```text
Attanasio Replication/Our Most Recent Output
```



##  What happens during a run

We proceed in four main steps.

### Step 1: Construct the measurement system

`OurR_MeasurementSpec_No4.r` reads the data and constructs six latent factors:

- Child human capital in periods 1, 2, and 3.
- Parental investment in periods 1, 2, and 3.


### Step 2: Estimate the latent-factor distribution

`Our_estim_meas_model.R` estimates the joint distribution of the six latent
factors and the observed household variables. The distribution is represented
as a two-component mixture of normal distributions.

The estimator iterates until its convergence criterion is sufficiently small.
During the run, the terminal prints messages such as:

```text
Iteration: 25
Convergence criterion: 0.0007
```

These messages are not the final production-function estimates.

### Step 3: Draw synthetic factor data

`Our_Procedures.R` draws synthetic observations from the estimated joint
distribution. The latent factors are exponentiated so they enter the
production-function stage as positive levels.

### Step 4: Estimate the production functions

`Our_Production_Functions_No4_Incomeinst.r` estimates one production function
for each childhood period.

The current specification is a nested CES technology. It allows:

- Incoming child human capital to combine with current inputs.
- Monetary and time investment to form a parental-input bundle.
- The parental-input bundle to combine with public investment.
- Parental education to affect productivity.
- A control function to address the possible endogeneity of parental
  investment.

For each period, the code estimates the production function both with and
without the control function. The control-function estimates are the estimates
exported to Rev3.

##  The main outputs

###  The  bridge

The most important output is:

```text
attanasio_rev3_bridge.csv
```

It contains exactly three rows, one for each modeled period:

| Column | Meaning |
|---|---|
| `period` | Production period, numbered 1 through 3. |
| `intercept` | Period-specific productivity intercept. |
| `parent_education_coefficient` | Effect of parental education on productivity. |
| `theta_h` | Weight on incoming child human capital in the outer CES nest. |
| `theta_p` | Weight on the parental-input bundle within the inner CES nest. |
| `ces_delta` | Monetary-investment share within the parental monetary/time bundle. |
| `mu` | Substitution parameter for parental and public inputs. |
| `rho` | Substitution parameter for incoming human capital and current inputs. |
| `control_function_coefficient` | Estimated coefficient on the first-stage residual. |
| `residual_sd` | Standard deviation of the production-function residual. |

Its shape is:

```text
period,intercept,parent_education_coefficient,theta_h,theta_p,ces_delta,mu,rho,control_function_coefficient,residual_sd
1,...
2,...
3,...
```

Rev3 checks that all three rows are present, all entries are finite, and the
CES parameters lie inside their permitted domains.

### Production-function tables

The files:

```text
cestable_1.csv
cestable_2.csv
cestable_3.csv
```

report the production-function results for each period. Each file has two
main estimate columns:

- Human capital without the control function.
- Human capital with the control function.

For each parameter, the table is arranged in three rows:

1. Point estimate.
2. Bootstrap standard error.
3. Bootstrap interval.

###  Parental-investment regressions

The files:

```text
investtable1.csv
investtable2.csv
investtable3.csv
```

summarize the reduced-form parental-investment equations used to construct the
control functions.

The more detailed files:

```text
inst1_regression_summary.csv
inst2_regression_summary.csv
inst3_regression_summary.csv
```

contain the coefficient, standard error, t statistic, p value, R-squared, and
F statistic for each first-stage regression.

### Factor-model files

The main saved factor-model objects are:

| File | Contents |
|---|---|
| `loadings.Rdata` | Estimated measurement factor loadings. The CEX loading is one for all three investment factors, and the PIAT Math loading is one for all three child-HC factors. |
| `constants.Rdata` | Measurement-equation constants. |
| `covariances.Rdata` | Estimated factor covariance matrices. |
| `means.Rdata` | Estimated factor means. |
| `eps.Rdata` | Measurement-error variances. |
| `trueFM.R` | Joint factor-distribution parameters used to draw the main synthetic data. |
| `allbootFM.R` | Bootstrap factor-model estimates. |
| `est.Rdata` | Full estimated measurement-model object. |

These files are R binary objects. They are intended to be loaded in R rather
than opened as spreadsheets.

###  Production-function R objects

The files `trueCESperiod1.R` through `trueCESperiod3.R` contain the main
production-function estimates. The corresponding `bootCES` files contain the
bootstrap estimates.

The `trueInvest`, `bootInvest`, `coef_ces`, and `bootcoef_ces` files contain
the first-stage and auxiliary regression results used to construct the
reported tables.



##  What a successful run looks like

A successful run has the following features:

1. R prints the number of complete observations and the number of measurement
   variables.
2. The factor estimator reaches its convergence criterion.
3. The production-function routines run for all three periods.
4. The terminal prints:

```text
Wrote Rev3 technology bridge: .../attanasio_rev3_bridge.csv
```

5. The output directory contains a three-row bridge and the period-specific
   CES, investment, regression, and factor-model files.


