# Performance Script

## Stage 1: Perform queries

### Usage

`
python3 queries.py -h
`

    usage: queries.py [-h] [-v] output_dir ipa_arr
    
    positional arguments:
        output_dir     Indicate the full path to where performance test results will be stored
        ipa_arr        Array containing URL to API service

    optional arguments:
       -h, --help     show this help message and exit
       -v, --verbose  Run the program in verbose mode.

## Stage 2: Generate plots from queries

### Usage

`
python3 plot_performance.py -h
`

    usage: plot_performance.py [-h] [-v] path s_date e_date
      
    positional arguments:
        path           Full path to directory where performance CSV results are stored
        s_date         Start Date of performance monitoring period. Format: YYYY-MM-DD
        e_date         End Date of performance monitoring period. Format: YYYY-MM-DD
      
     optional arguments:
        -h, --help     show this help message and exit
        -v, --verbose  Run the program in verbose mode.
