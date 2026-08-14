### Adaptive Metropolis with Simulated Method of Moments
Written by Bijan Taheri
## Quick Links

Bijan's Resources for Learning SMM/MH: https://docs.google.com/document/d/13ntaRxTZ_TXD4RHun7ssRopB5kzpCi2x3dAITKbJl6w/edit?usp=sharing

Michael/Bijan's Overleaf doc for explaining/justifying SMM with MH: https://www.overleaf.com/project/68898676b365d915746df5ec

Parameter Estimation Methodology: https://www.overleaf.com/project/688ce58d7f15b3734fce3f33

## History

This section details a quick history of writing the SMM algorithm. 

# Summer 2025: Work by Bijan

During the summer of 2025, in addition to the code to clean the data, Bijan wrote the initial code for the Simulated Method of Moments algorithm. This code, employing Michael's household solver (labeled "solve_system.py" at the time), can be found in "Del_Boca_Implementation.py" (Bijan's copy, NOT under "Aaron's code"). Within a week or two, after some extensive testing on Firebird (Swarthmore's supercomputer - this is what the submission scripts are for), the team had realized that running such an algorithm on actual data would be far too time-intensive. So, Michael came up with the idea for combining Simulated Method of Moments with a sampling algorithm, specifically Metropolis-Hastings. Based on this, and after further research and testing, Bijan (with the significant help of AI) wrote an adaptive Metropolis algorithm with Simulated Method of Moments ("adaptive_metropolis.py"), which drew on the "Del_Boca_Implementation.py" file for its SMM-related functions. 

# Last week of summer - Aaron's additions

Due to scheduling, Aaron started his internship a few weeks after Bijan. Aaron completed a couple additional tasks with Bijan's code: he shortened the model to 3 periods (as it used to be 4 periods, but the team decided to change our approach to 3 periods to simplify the dimensionality of our parameter search, among other reasons), and he translated the code into Julia (as Julia is much, much faster computationally and allows for multithreading). In making these changes, he created copies of all the Del Boca files, located under "Aaron's Code." More details on his changes to the algorithm can be found in the "README.docx" file under the "Aaron's Code" folder. Importantly, the three-period solver which should accompany this algorithm has yet to be finished; in its place currently is a test 3-period solver, NOT an actual one. 

# Joshua's Work Summer 2026

During the summer of 2026, Joshua, with extensive help from AI, updated Aaron's Julia code into the current Rev3 implementation and connected it to a completed three-period parent policy solver. The solver was adjusted to use the nested-CES human capital production function described in the project methodology, to solve over both child and parent human capital, and to distinguish between the technology parents believe and the true technology that actually determines the child's next-period human capital. The true period-specific technology estimates now come from the Attanasio code through a small CSV bridge, while Del Boca estimates Delta, a common perceived rho, and three period-specific perceived theta_h values. The SMM and adaptive Metropolis files were updated to use this same five-parameter setup throughout. After testing several global optimizers, the code was changed to use threaded DX-NES for both stages of SMM, followed by adaptive Metropolis. To make the code more practical for a large number of households, each candidate parameter vector now solves one reusable policy grid and then interpolates and simulates households in parallel, rather than solving the full household problem separately for every household. Joshua also replaced the placeholder data moments with moments constructed from the merged NLSY/CEX data, including CEX education expenditure, labor income, public inputs, leisure, and child human capital; added shared and changeable paths for the Mac and eventual Windows VDE environments; and added dry-run, smoke-test, and substantive-estimation scripts. Finally, the current Rev3 files were moved into the main Del Boca folder, while the older Python, Rev2, test-solver, benchmark, and historical-output files were kept in the "Old" folder for reference.

# Updates since

This code remained largely untouched in fall and winter of 2025, with work on the VDE becoming the priority. In spring of 2026, Bijan was able to upload Aaron's version of the Del Boca code to the VDE, specifically the "adaptive_metropolis.py," "Del_Boca_Replication.py," and "solve_system_3period.py." The reason for including Python files rather than Julia is due to the fact that the VDE cannot run Julia; after a number of emails back and forth, the manager of the VDE told us that they would not be able to implement Julia. Small changes were made to these files for paths and such, but the extent of the work on these still remains to be done (as discussed below).

## TODO

There are many tasks to be done with regard to the Del Boca code. The major task is adjusting the code so that it works with the empirical data in the VDE. When Bijan wrote the code, he inserted simulated data as a placeholder for empirical data (as the team did not have access to the VDE at the time). So, most of the work adjusting the code for the VDE comes in the form of replacing "real" (simulated) data for actual data in the VDE. Specifically, the "create_households" function in "Del_Boca_Implementation.py" file needs to be adjusted to reflect real-life households, incorporating the regressions here: https://docs.google.com/document/d/10GxXUYNQ6FM6RI33fXhSt8qnfnVHUYzclGlQshYNbDQ/edit?usp=sharing. Also, empirical moments (which, when Bijan was talking with Aaron over the summer, should come directly from the Attanasio code) need to be loaded into the algorithm. 

Additionally, the code needs to be adjusted for parameters to vary over the 3 periods. Right now, theta_1, theta_2, and theta_3 represent parameters for different preferences of expenditures, child human capital, and leisure (not necessarily in that order), and there is only one rho value. Part of this is already outlined in the code, but these parameters need to vary across periods (i.e., have a different parameter for each of these in period 1, 2, and 3). 

Finally, there are many minor adjustments that can be made to the algorithm to be in line with Haario et al. (2001). These are mostly written in the form of "TODO" statements at the top of the code file. As one of these TODOs, perhaps the largest task of all of these is checking over the algorithm to ensure that, when coding, the AI did not make any mistakes. 



## Final note

If anything doesn't make sense in this part of the file, let Bijan know! He is happy to explain it. His email is btaheri1@swarthmore.edu.
