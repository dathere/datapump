# datapump
pump data into the CKAN datastore using simple filesystem based queueing.

Requires: Python 3.8

Installation:
=============

```
python3 -m venv datapumpenv
. datapumpenv/bin/activate
cd datapumpenv
git clone https://github.com/dathere/datapump.git
pip install -r requirements.txt
```

Usage:
======

```
. datapumpenv/bin/activate
cd datapumpenv
python datapump.py --config datapump.ini
```

Command line parameters:
------------------------

```
python datapump.py --help
Usage: datapump.py [OPTIONS]

  Pumps data into CKAN using a simple directory-based queueing system.

Options:
  --inputdir PATH      The directory where the job files are located.  [default: ./input]
  --processeddir PATH  The directory where successfully processed job files are moved.  [default: ./processed]
  --problemsdir PATH   The directory where unsuccessful job files are moved.  [default: ./problems]
  --host TEXT          CKAN host.  [required]
  --apikey TEXT        CKAN api key to use.  [required]
  --verbose            Show more information while processing.
  --debug              Show debugging messages.
  --logfile PATH       The full path of the log file.  [default: ./datapump.log]
  --config FILE        Read configuration from FILE.
  --help               Show this message and exit.
```

Note that parameters can be passed in priority order - through environment variables, a config file, or through the command line interface.

Environment variables should be all caps and prefixed with `DATAPUMP_`, for example:

```
export DATAPUMP_APIKEY="MYCKANAPIKEY"
export DATAPUMP_HOST="https://ckan.example.com"
```
