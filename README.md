# Data Processing of MA Project
This Github repo contains the data pre-processing part of the MA Project.
- This repo does not contain all the data file due to the size restriction of Github(The data file should be loaded seperatly).




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
------ [filename].pickle # where the cache pickle file stored in
-- MA
---- <where all the code stored, everything on Github>

```

---
## Data Sources

- Thomason Reuter's SDC Platinum
    - provide comprehensive MA event data
- Compustat (financial variable)
    - Unfortunately, there are a lot missing values in this dataset(poor quality). I suggest big data guy should not consider using this database.
- EDGAR (financial disclosures)
    - operated by SEC. Since the annual disclosure (10-K) is required by law. There is no missing data. However, they are all raw textutal data.
- TNIC (Text-based Network Industry Classifications Data)
    - TNIC is created based on bag-of-words (may seems to be a out-of-date NLP technique in Machine Learning area.). However, TNIC is surprisingly popular in Finance area.

## Usage

Please Run all `Master` files to generate approparate dataset for predicting Merger and Acquisition. The generated `dataset.pkl` file can be further loaded via Pytorch DataLoader Object in [the modeling repository](https://github.com/dayuyang1999/Merger_Acquisition_Prediction).
