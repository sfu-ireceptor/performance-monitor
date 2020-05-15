# performance-monitor
Scripts to monitor the performance of the AIRR Data Commons

# Performance Testing and Data Provenance Scripts 	

This repository contains several python and shell scripts that perform data provenance checks on iReceptor API services, as well as performance tests on iReceptor API services and ADC API services. 	

## Directory Structure	

    ├── ADC-API-Data-provenance
    │   ├── Rearrangement_Tests
    │   │   ├── adc_api_datatest.py
    │   │   ├── curlairripa.py
    │   │   └── README.md
    │   └── Repertoire_Tests
    │       ├── AIRR-iReceptorMapping-v1.3-2020-01-16.txt
    │       ├── AIRR-repertoire-checks.py
    │       ├── curlairripa.py
    │       ├── master_metadata_2019-09-17.xlsx
    │       ├── nofilters.json
    │       ├── nofilters_OUT.json
    │       └── README.md
    ├── ADC-API-Performance
    │   ├── adc_api_performancetest.py
    │   ├── adc_api_plot_performance.py
    │   ├── curlairripa.py
    │   ├── README.md
    │   └── run_queries.sh
    ├── API_Performance_Scripts
    │   ├── DataProvenance
    │   │   ├── productive_test.py
    │   │   ├── README.md
    │   │   ├── sequences.py
    │   │   └── service_IPA3_OddWords.sh
    │   ├── Performance
    │   │   ├── plot_performance.py
    │   │   ├── queries.py
    │   │   ├── README.md
    │   │   ├── run_queries.sh
    │   │   └── SamplePlots
    │   │       ├── QueryTimesAllQueriesallqueries2019110520191205.png
    │   │       ├── QueryTimesSelectedQueriesALLIPASOnePlot2019110520191205.png
    │   │       ├── TotalSequenceSampleDistributionALLIPAS.png
    │   │       └── TotalSequenceSamplesALLIPAS.png
    │   └── README.md
    ├── LICENSE
    ├── PLOTS
    │   ├── ADC-API
    │   │   └── README.md
    │   └── IPA1-IPA2-IPA3-IPA4-IPA5
    │       ├── QueryTimes(AllQueries_all queries)__ 2020-04-14-2020-05-14.png
    │       ├── QueryTimes(SelectedQueries_ALL_IPASOnePlot)__ 2020-04-14-2020-05-14.png
    │       ├── TotalSequenceSampleDistributionALLIPAS.png
    │       └── TotalSequenceSamplesALLIPAS.png
    ├── README.md
    └── requirements.txt


## Requirements	

Python 3.5 or higher. 	



