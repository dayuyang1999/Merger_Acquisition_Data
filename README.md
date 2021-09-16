# MA
This Github repo contains Code part of the MA Project.
- This repo does not contain all the data file.(The data file should be loaded seperatly)

---
## Folder Structure
By default, the overall dir should be like this:

```root
root
-- MA_data
---- SDC
------ [year].xlsx # the annually Merge and Acquisition xlsx file downloaded from SDC Platinum DataBase
---- wrds_bridge.csv # the GVKEY-CUSIP Linking table downloaded from WRDS
---- evans_bridge.csv # 
---- tmp
------ where the cache pickle file stored in
-- MA
---- <where all the code stored, everything on Github>

```
