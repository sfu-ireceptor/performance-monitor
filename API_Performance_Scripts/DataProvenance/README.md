# TSV Data Provenance Test

## Data provenance checks I:

Test coverage: 

- Measure how long it takes to complete full TSV download (all sequenced in a single sample)
- Check TSV passes AIRR standards
- Check for the presence of 'odd words' in v_call, d_call, j_call fields.  
- Check locus equality (for a given sequence, first three characters in v_call, d_call, j_call coincide)
- For a given sequence, check length of junction_aa matches junction_aa_length match

### Usage

`python3 sequences.py -h`

    usage: sequences.py [-h] [-v] sample_id words TSV_dir base_url

    positional arguments:
        sample_id      Corresponds to value found in _id API iReceptor Service value
        words          Comma-separated strings containing odd words to search for
        TSV_dir        Full path where TSV files are stored
        base_url       URL pointing at API iReceptor Service, e.g.https://ipa1.ireceptor.org/

    optional arguments:
        -h, --help     show this help message and exit
        -v, --verbose  Run the program in verbose mode.

## Data provenance checks II:

Test coverage:

- Check productive field does not return NaN value

### Usage

`python3 productive.py -h`

    usage: productive_test.py [-h] [-v] TSV_dir _id

    positional arguments:
      TSV_dir        Full path where TSV files are stored
      _id            Corresponds to value found in _id API iReceptor Service value

    optional arguments:
      -h, --help     show this help message and exit
      -v, --verbose  Run the program in verbose mode.

