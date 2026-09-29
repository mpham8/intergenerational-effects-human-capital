### Bijan's Work (VDE + Regressions)
Written by Bijan Taheri
btaheri1@swarthmore.edu
Spring 2026

## In the VDE

Bijan's work in the VDE has mainly been translating code over from outside the VDE. He was able to translate the data cleaning code to the VDE, adding specific lines to process the geocodes. He modified Aaron's "Fake_Merging_Data.py" file to actually merge the outside FIPS-relevant data with the NLSY data using the geocodes, renaming it to "Real_Merging_Data.py." He also added both the Attanasio and Del Boca replication files to the VDE, changing paths as needed for the Attanasio code to work. 

# TODO

The biggest task for the VDE (not covered in my Del Boca replication README) is understanding the Attanasio code. In particular, there is one line (which I showed Josh in the VDE) that seems to be causing NAN errors, but I did not have the experience in R to figure out the reason why. Since I was not involved in the Attanasio process, I struggled to understand the outputs from the code. Understanding these outputs is key not only for this section of the project, but also for piecing the latent factor analysis into the Del Boca replication. 

## Regressions

For the Del Boca replication, specifically creating the simulated households, I created a regression file, "AR1_regressions.do," in the "regressions" folder. This file is designed to run the regressions laid out in this document: https://docs.google.com/document/d/10GxXUYNQ6FM6RI33fXhSt8qnfnVHUYzclGlQshYNbDQ/edit?tab=t.0. The .do file has not been fully checked to ensure the code AI has generated aligns with the Google Doc in full, so this represents the next TODO for this side of the project. 