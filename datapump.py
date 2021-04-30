import logging
from colorama import Fore, Back, Style
import click
import click_config_file
from ckanapi import RemoteCKAN
import pandas as pd
import json
import os
import glob
from datetime import datetime
from time import perf_counter
import shutil
import dateparser

formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')


def setup_logger(name, log_file, level=logging.INFO):
    """To setup as many loggers as you want"""

    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger


@click.command(context_settings=dict(max_content_width=120))
@click.option('--inputdir',
              type=click.Path(exists=True),
              default='./input',
              show_default=True,
              help='The directory where the job files are located.')
@click.option('--processeddir',
              type=click.Path(exists=True),
              default='./processed',
              show_default=True,
              help='The directory where successfully processed job files are moved.')
@click.option('--problemsdir',
              type=click.Path(exists=True),
              default='./problems',
              show_default=True,
              help='The directory where unsuccessful job files are moved.')
@click.option('--datecolumn',
              default='DateTime',
              show_default=True,
              help='The name of the datetime column.')
@click.option('--dateformats',
              default='%y-%m-%d %H:%M:%S, %y/%m/%d %H:%M:%S, %Y-%m-%d %H:%M:%S, %Y/%m/%d %H:%M:%S',
              show_default=True,
              help='List of dateparser format strings to try one by one. See https://dateparser.readthedocs.io')
@click.option('--host',
              required=True,
              help='CKAN host.')
@click.option('--apikey',
              required=True,
              help='CKAN api key to use.')
@click.option('--verbose',
              is_flag=True,
              help='Show more information while processing.')
@click.option('--debug',
              is_flag=True,
              help='Show debugging messages.')
@click.option('--logfile',
              type=click.Path(),
              default='./datapump.log',
              show_default=True,
              help='The full path of the log file.')
@click_config_file.configuration_option(config_file_name='datapump.ini')
def datapump(inputdir, processeddir, problemsdir, datecolumn, dateformats,
             host, apikey, verbose, debug, logfile):
    """Pumps time-series data into CKAN using a simple filesystem-based
    queueing system."""

    ua = 'datapump/1.0'
    dateformats_list = dateformats.split(', ')

    logger = setup_logger('mainlogger', logfile,
                          logging.DEBUG if debug else logging.INFO)
    processed_logger = setup_logger(
        'processedlogger', processeddir + '/processed.log')
    problems_logger = setup_logger(
        'problemslogger', problemsdir + '/problems.log')

    # helper for logging to file and console
    def logecho(message, level='info'):
        if level == 'error':
            logger.error(message)
            click.echo(Fore.RED + level.upper() + ': ' + Fore.WHITE +
                       message, err=True) if verbose else False
        elif level == 'warning':
            logger.warning(message)
            click.echo(Fore.YELLOW + level.upper() + ': ' +
                       Fore.WHITE + message) if verbose else False
        elif level == 'debug':
            logger.debug(message)
            click.echo(Fore.GREEN + level.upper() + ': ' +
                       Fore.WHITE + message) if debug else False
        else:
            logger.info(message)
            click.echo(message)

    logecho('DATEFORMATS: %s' % dateformats_list, level='debug')

    def get_col_dtype(col):
        if col.dtype == "object":

            try:
                col_new = pd.to_datetime(col.dropna().unique())
                return ['timestamp', 'datetime']
            except:
                return ["text", 'string']

        elif col.dtype == 'float64':
            return ['float', 'float64']
        elif col.dtype == 'int64':
            return ['int', 'int64']
        elif col.dtype == 'datetime64[ns]':
            return ['timestamp', 'datetime']
        else:
            return ['text', 'string']

    # helper for reading job json file
    def readjob(job):
        with open(job) as f:
            try:
                jobdefn = json.load(f)
            except ValueError as e:
                return False
            else:
                return jobdefn

    # helper for running jobs
    def runjob(job):

        inputfiles = glob.glob(job['InputFile'])
        logecho('  %s file/s found for %s: ' %
                (len(inputfiles), job['InputFile']))

        # process files, order by most recent
        inputfiles.sort(key=os.path.getmtime, reverse=True)
        for inputfile in inputfiles:
            inputfile_error = False
            inputfile_errordetails = ''
            t1_startdt = datetime.now()
            t1_start = perf_counter()
            dupecount = 0
            dupesremoved = 0

            logecho('    Processing: %s...' % inputfile)

            def custom_date_parser(x): return dateparser.parse(
                x, date_formats=dateformats_list)

            df = pd.read_csv(inputfile, parse_dates=[
                             datecolumn], date_parser=custom_date_parser)

            if job['Dedupe']:
                pkey_list = list(job['PrimaryKey'].split(','))

                # first, count number of dupe rows for logging
                dupecount = df.duplicated(subset=pkey_list, keep='first').sum()

                dedupe_flag = job['Dedupe'].lower()
                if dedupe_flag == 'first' or dedupe_flag == 'last':
                    df.drop_duplicates(
                        subset=pkey_list, keep=dedupe_flag, inplace=True)
                    dupesremoved = dupecount

            colname_list = df.columns.tolist()

            coltype_list = []
            for column in df:
                coltype_list.append(get_col_dtype(df[column]))

            fields_dictlist = []
            for i in range(0, len(colname_list)):
                fields_dictlist.append({
                    "id": colname_list[i],
                    "type": coltype_list[i][0]
                })
                if coltype_list[i][0] == 'timestamp':
                    df[colname_list[i]] = df[colname_list[i]].astype(str)

            logecho('FIELDS_DICTLIST: %s' % fields_dictlist, level='debug')

            data_dict = df.to_dict(orient='records')

            # check if resource exists
            # this works only when TargetResource is an existing
            # resource id hash
            try:
                resource = portal.action.resource_show(
                    id=job['TargetResource'])
            except:
                logecho('    Resource "%s" is not a resource id.' %
                        job['TargetResource'])
                resource = ''

            if not resource:
                # resource doesn't exist. Check if package exists
                try:
                    package = portal.action.package_show(
                        id=job['TargetPackage'])
                except:
                    package = ''

                if not package:
                    # package doesn't exist. Create it
                    # first, check if TargetOrg exist
                    logecho('    Creating package "%s"...' %
                            job['TargetPackage'])

                    if not (job['TargetOrg'] in org_list):
                        logecho('TargetOrg "%s" does not exist!' %
                                job['TargetOrg'], level='error')
                        sys.exit()

                    try:
                        package = portal.action.package_create(
                            name=job['TargetPackage'],
                            private=False,
                            owner_org=job['TargetOrg']
                        )
                    except Exception as e:
                        logecho('    Cannot create package "%s"!' %
                                job['TargetPackage'], level='error')
                        inputfile_error = True
                        inputfile_errordetails = str(e)
                        package = ''
                    else:
                        logecho('    Created package "%s"...' %
                                job['TargetPackage'])
                else:
                    logecho('    Package "%s" found...' % job['TargetPackage'])

                logecho('PACKAGE: %s\n\nFIELDS: %s' %
                        (package, fields_dictlist), level='debug')
                # logecho('RECORDS: %s\n' % data_dict, level='debug')

                # now check if resource name already exists in package
                resource_exists = False
                resources = package.get('resources')
                for resource in resources:
                    if resource['name'] == job['TargetResource']:
                        resource_exists = True
                        break

                if package and resource_exists:
                    logecho('    "%s" exists in package "%s". Doing datastore_upsert...' % (
                        job['TargetResource'], job['TargetPackage']))
                    try:
                        result = portal.action.datastore_upsert(
                            resource_id=resource['id'],
                            records=data_dict,
                            method='upsert',
                            calculate_record_count=True
                        )
                    except Exception as e:
                        logecho('    Upsert failed', level='error')
                        inputfile_error = True
                        inputfile_errordetails = str(e)
                    else:
                        logecho('    Upsert successful! %s rows...' %
                                len(data_dict))
                else:
                    logecho('    "%s" does not exist in package "%s". Doing datastore_create...' % (
                        job['TargetResource'], job['TargetPackage']))

                    resource = {
                        "package_id": package['id'],
                        "format": "csv",
                        "name": job['TargetResource']
                    }
                    try:
                        resource = portal.action.datastore_create(
                            resource=resource,
                            fields=fields_dictlist,
                            records=data_dict,
                            primary_key=job['PrimaryKey'],
                            indexes=job['PrimaryKey']
                        )
                    except Exception as e:
                        logecho('    Cannot create resource "%s"!' %
                                job['TargetResource'], level='error')
                        inputfile_error = True
                        inputfile_errordetails = str(e)
                    else:
                        logecho('    Created resource "%s"...' %
                                job['TargetResource'])

            logecho('RESOURCE: %s' % resource, level='debug')

            t1_stop = perf_counter()
            t1_stopdt = datetime.now()
            starttime = t1_startdt.strftime('%Y-%m-%d %H:%M:%S')
            endtime = t1_stopdt.strftime('%Y-%m-%d %H:%M:%S')
            elapsed = t1_stop - t1_start

            if inputfile_error:
                # inputfile processing failed, move to problemsdir
                try:
                    shutil.move(inputfile, problemsdir)
                except Exception as e:
                    errmsg = 'Cannot move %s to %s: %s' % (
                        inputfile, problemsdir, str(e))
                    logecho(errmsg, level='error')
                    problems_logger.error(errmsg)

                error_details = '- FILE: %s START: %s END: %s ELAPSED: %s DUPES: %s/%s ERRMSG: %s' % (
                    inputfile, starttime, endtime, elapsed, dupecount, dupesremoved, inputfile_errordetails)
                problems_logger.info(error_details)
            else:
                # inputfile was successfully processed, move to processeddir
                try:
                    shutil.move(inputfile, processeddir)
                except Exception as e:
                    errmsg = 'Cannot move %s to %s: %s' % (
                        inputfile, processeddir, str(e))
                    logecho(errmsg, level='error')
                    processed_logger.error(errmsg)

                processed = len(df.index) if 'df' in locals() else 0
                processed_details = '- FILE: %s START: %s END: %s ELAPSED: %s DUPES: %s/%s PROCESSED: %s' % (
                    inputfile, starttime, endtime, elapsed, dupecount, dupesremoved, processed)
                processed_logger.info(processed_details)

        logecho('  Processed %s file/s...' % len(inputfiles))

    # datapump func main
    logecho('Starting datapump...')

    # log into CKAN
    try:
        portal = RemoteCKAN(host, apikey=apikey, user_agent=ua)
    except:
        logecho('Cannot connect to host %s' %
                host, level='error')
        sys.exit()
    else:
        logecho('Connected to host %s' % host)

    org_list = portal.action.organization_list()

    # read jobs
    jobs = os.scandir(inputdir)
    for job in jobs:
        if (not job.name.startswith('.') and job.name.endswith('-job.json') and
                job.is_file()):
            logecho('  Reading job - %s' % job)
            jobdefn = readjob(job)
            if jobdefn:
                logecho(json.dumps(jobdefn), level='debug')
                runjob(jobdefn)
            else:
                logecho('  Invalid job json', level='error')

    logecho('Ending datapump...')


if __name__ == '__main__':
    datapump(auto_envvar_prefix='DATAPUMP')
