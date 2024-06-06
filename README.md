# RiskAdjustment

Python code to query the RxNorm API and map provided NDC11 codes to their ATC classification.

### Getting started

The code requires the following python libraries: `pandas`, `numpy`, `requests`. Once you've forked the repository, you should be able to import the module and run the main function `main_query_from_new_ndc11` with a list of NDC11 codes that you want to map. 

```
import pandas as pd
import query_rx as qrx

# list of NDCs
new_ndc11 = ['00002759701', '00169450101', '00169450114']

qrx.main_query_from_new_ndc11(new_ndc11)
```

Once this method runs, it will create the cleaned mapping in `data\clean\ndc_to_atc.pkl` 

```
df = pd.read_pickle(qrx.Params().clean_path+'ndc_to_atc.pkl')
display(df[['drug_ndc', 'ATC']])

#      drug_ndc      ATC
#0  00002759701  N05AH03
#1  00169450101  A10BJ06
#2  00169450114  A10BJ06
```

A more throrough walkthrough of the code can be found in the [**Walkthrough**](https://github.com/Yale-Medicaid/Query_RxNorm/blob/main/Walkthrough.ipynb).