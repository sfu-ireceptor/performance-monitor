# TSV Data Provenance Test

### USage

`python3 sequences.py -h`

usage: sequences.py [-h] [-v] sample_id words TSV_dir base_url

positional arguments:
  sample_id      Corresponds to value found in _id API iReceptor Service value
  words          Comma-separated strings containing odd words to search for
  TSV_dir        Full path where TSV files are stored
  base_url       URL pointing at API iReceptor Service, e.g.
                 https://ipa1.ireceptor.org/

optional arguments:
  -h, --help     show this help message and exit
  -v, --verbose  Run the program in verbose mode.
