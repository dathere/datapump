import logging
from colorama import Fore, Back, Style
import click
import click_config_file
from ckanapi import RemoteCKAN
import pandas as pd
from pandas.tseries.frequencies import to_offset
import json
import sys
import os
import glob
from datetime import datetime
from time import perf_counter
import shutil
import dateparser
from jsonschema import validate
import re

version = '1.1'
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

jobschema = {
    "$schema": "http://json-schema.org/draft-07/schema#",

    "definitions": {
        "Stat": {
            "type": "object",
            "properties": {
                "Kind": {"type": "string"},
                "GroupBy": {"type": "string"},
                "DropColumns": {"type": "string"}
            }
        }
    },

    "type": "object",
    "properties": {
            "InputFile": {"type": "string"},
            "TargetOrg": {"type": "string"},
            "TargetPackage": {"type": "string"},
            "TargetResource": {"type": "string"},
            "PrimaryKey": {"type": "string"},
            "Dedupe": {"type": "string", "enum": ["first", "last"]},
            "Truncate": {"type": "boolean"},
            "Stats": {"type": "array",
                      "contains": {
                          "$ref": "#/definitions/Stat"
                      }
                      }
    },
    "required": ["InputFile", "TargetOrg", "TargetPackage", "TargetResource",
                 "PrimaryKey", "Dedupe"]
}


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
              help='The directory where successfully processed inputfiles are moved.')
@click.option('--problemsdir',
              type=click.Path(exists=True),
              default='./problems',
              show_default=True,
              help='The directory where unsuccessful inputfiles are moved.')
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
              help='The full path of the main log file.')
@click_config_file.configuration_option(config_file_name='datapump.ini')
@click.version_option(version)
def datapump(inputdir, processeddir, problemsdir, datecolumn, dateformats,
             host, apikey, verbose, debug, logfile):
    """Pumps time-series data into CKAN using a simple filesystem-based
    queueing system."""

    dateformats_list = dateformats.split(', ')

    logger = setup_logger('mainlogger', logfile,
                          logging.DEBUG if debug else logging.INFO)
    processed_logger = setup_logger(
        'processedlogger', processeddir + '/processed.log')
    problems_logger = setup_logger(
        'problemslogger', problemsdir + '/problems.log')
    job_logger = setup_logger(
        'joblogger', inputdir + '/job.log')
    global data_df

    def logecho(message, level='info'):
        """helper for logging to file and console"""
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
        """helper to get column data type from dataframe
        it returns both the CKAN friendly python/postgres data type
        that can be used for datastore_create and the numpy data type"""
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

    def computestats(stats, primarykey, package, resource_id, resource_name, updatetime):
        """ all, hourly, daily, lastndays, weekly, monthly, quarterly, annually, todate """

        def savestat(saveinfo):

            stats_dict = saveinfo[0]
            fields_dictlist = saveinfo[1]
            stats_name = saveinfo[2]
            stats_desc = saveinfo[3]
            primary_key = saveinfo[4]
            logecho('STATS DATA: %s' % stats_dict, level='debug')

            stats_exists = False
            stats_error = False

            logecho('STATS PACKAGE: %s' % package, level='debug')

            resources = package.get('resources')
            for resource in resources:
                if resource['name'] == stats_name:
                    stats_exists = True
                    stats_resource_id = resource['id']
                    break

            if stats_exists:
                logecho('STATS RESOURCE: %s' % resource, level='debug')

                logecho('    Replacing old %s, %s' %
                        (stats_name, stats_resource_id))
                try:
                    result = portal.action.datastore_upsert(
                        force=True,
                        resource_id=stats_resource_id,
                        records=stats_dict,
                        method='upsert',
                        calculate_record_count=True
                    )
                except Exception as e:
                    logecho('    Stats upsert failed',
                            level='error')
                    stats_error = True
                    stats_errordetails = str(e)
                else:
                    logecho('STATS RESULT FOR CREATE: %s' %
                            result, level='debug')

            else:
                resource = {
                    "package_id": package['id'],
                    "format": "csv",
                    "name": stats_name
                }

                logecho('STATS RESOURCE FOR CREATE: %s' %
                        resource, level='debug')

                alias = '%s-%s-%s' % (package['organization']['name'],
                                      package['name'], stats_name)

                try:
                    result = portal.action.datastore_create(
                        force=True,
                        resource=resource,
                        aliases='',
                        fields=fields_dictlist,
                        primary_key=primary_key,
                        records=stats_dict,
                        calculate_record_count=False
                    )
                except Exception as e:
                    logecho('    Cannot create resource "%s"!' %
                            result, level='error')
                    stats_error = True
                    stats_errordetails = str(e)
                else:
                    logecho('    Created resource "%s"...' %
                            result, level='debug')
                    stats_resource_id = result['resource_id']

                    result = portal.action.datastore_create(
                        force=True,
                        resource_id=stats_resource_id,
                        aliases=alias,
                        calculate_record_count=True
                    )

            if stats_error:
                return '- STAT: %s ERRMSG: %s' % (stats_name, stats_errordetails)
            else:
                portal.action.resource_update(
                    id=stats_resource_id,
                    description='%s (UPDATED: %s)' % (stats_desc, updatetime))

                return False

        def getFields(stats_df):
            # get column names and data types
            # we need this to create the datastore table
            colname_list = stats_df.columns.values.tolist()

            coltype_list = []
            for column in stats_df:
                coltype_list.append(get_col_dtype(stats_df[column]))

            fields_dictlist = []
            for i in range(0, len(colname_list)):
                fields_dictlist.append({
                    "id": colname_list[i],
                    "type": coltype_list[i][0]
                })
                if coltype_list[i][0] == 'timestamp':
                    stats_df[colname_list[i]
                             ] = stats_df[colname_list[i]].astype(str)

            logecho('STATS FIELDS_DICTLIST: %s' %
                    fields_dictlist, level='debug')

            return fields_dictlist

        def computefreqstat(freqKind, freqGroupBy, freqDropColumns):
            """ resample time series dataa """
            logecho("computing frequency resampling", level="debug")

            if freqDropColumns:
                dropcol_list = list(freqDropColumns.split(','))
                data_df.drop(columns=dropcol_list, inplace=True)
            freq_df = data_df.groupby(freqGroupBy).resample(freqKind).mean()
            stats_name = resource_name + '-' + freqKind
            stats_desc = 'Resampled - [%s](https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases)' % freqKind
            # delete internal _id column
            del freq_df[freq_df.columns[0]]

            freq_df.droplevel(0).reset_index(inplace=True)

            try:
                freq_df.drop(columns=list(freqGroupBy.split(',')), inplace=True)
            except Exception as e:
                logecho('    WARNING: %s' % e)

            freq_df.to_csv(stats_name + '-' + freqKind + '.csv')

            freq2_df = pd.read_csv(stats_name + '-' + freqKind + '.csv', na_values='')
            stats_dict = freq2_df.to_dict(orient='records')

            fields_dictlist = getFields(freq2_df)

            stats_sparse_dict = []
            for stat_record in stats_dict:
                sparse_record = {}
                for (k, v) in stat_record.items():
                    if not pd.isnull(v):
                        sparse_record[k] = v
                stats_sparse_dict.append(sparse_record)

            logecho('STATS DATA: %s' % stats_sparse_dict, level='debug')

            if not debug:
                os.remove(stats_name + '-' + freqKind + '.csv')

            return [stats_sparse_dict, fields_dictlist, stats_name, stats_desc, primarykey]

        def computedescstat(mode):

            if mode == 'descriptive':
                stats_df = data_df.describe(
                    include='all', datetime_is_numeric=True)
                stats_name = resource_name + '-stats'
                stats_desc = 'Descriptive statistics.'
                del stats_df[stats_df.columns[0]]
            else:
                stats_df = data_df.drop(data_df.columns[[0]], axis=1)
                stats_df = stats_df.mode()
                stats_name = resource_name + '-mode'
                stats_desc = 'Mode - values that appears most often.'

            stats_df.index.name = 'stat'
            stats_df.to_csv(stats_name + '.csv', index=True)

            # we do the save and load to csv to get around pandas' problem
            # of using numpy datatypes instead of native python data types when
            # running pandas describe()
            stats2_df = pd.read_csv(stats_name + '.csv', na_values='')
            stats_dict = stats2_df.to_dict(orient='records')

            # now get column names and data types
            # we need this when calling datastore_create
            fields_dictlist = getFields(stats_df)
            if mode == 'descriptive':
                fields_dictlist = [{ "id": "stat", "type": "text" }] + fields_dictlist

            stats_sparse_dict = []
            for stat_record in stats_dict:
                sparse_record = {}
                for (k, v) in stat_record.items():
                    if not pd.isnull(v):
                        sparse_record[k] = v
                stats_sparse_dict.append(sparse_record)

            logecho('STATS DATA: %s' % stats_sparse_dict, level='debug')

            if not debug:
                os.remove(stats_name + '.csv')

            return [stats_sparse_dict, fields_dictlist, stats_name, stats_desc, 'stat']

        # -----------  computestats MAIN --------------
        logecho('    Retrieving complete %s file...' % resource_name)
        data_df = pd.read_csv(host+'/datastore/dump/' +
                              resource_id, parse_dates=[datecolumn], index_col=[datecolumn])

        for stat in stats:
            logecho('    Computing stat - %s' % stat["Kind"])
            if stat["Kind"] == 'descriptive':
                stat_info = computedescstat('descriptive')
                statsave_error = savestat(stat_info)
            elif stat["Kind"] == 'mode':
                stat_info = computedescstat('mode')
                statsave_error = savestat(stat_info)
            else:
                try:
                    to_offset(stat["Kind"])
                except Exception as e:
                    logecho('    Invalid Stats Frequency',
                            level='error')
                    statsave_error = str(e)
                else:
                    stat_info = computefreqstat(
                        stat["Kind"], stat["GroupBy"], stat["DropColumns"])
                    statsave_error = savestat(stat_info)
            if statsave_error:
                problems_logger.info(statsave_error)

        return

    def readjob(job):
        """helper for reading job json file"""
        with open(job) as f:
            try:
                jobdefn = json.load(f)
            except ValueError as e:
                job_logger.error('Cannont load job json: %s', e)
                return False
            else:
                try:
                    validate(instance=jobdefn, schema=jobschema)
                except Exception as e:
                    logecho(str(e), level='error')
                    return False
                else:
                    return jobdefn

    def runjob(job):
        """helper for running jobs"""
        inputfiles = glob.glob(job['InputFile'])
        logecho('  %s file/s found for %s: ' %
                (len(inputfiles), job['InputFile']))

        # process files, order by most recent
        inputfiles.sort(key=os.path.getmtime, reverse=True)
        for inputfile in inputfiles:
            inputfile_error = False
            inputfile_errordetails = ''
            t1_startdt = datetime.now()
            starttime = t1_startdt.strftime('%Y-%m-%d %H:%M:%S')
            t1_start = perf_counter()
            dupecount = 0
            dupesremoved = 0
            resource_id = ''

            logecho('    Processing: %s...' % inputfile)

            def custom_date_parser(x): return dateparser.parse(
                x, date_formats=dateformats_list)

            df = pd.read_csv(inputfile, parse_dates=[
                             datecolumn], date_parser=custom_date_parser)

            if job['Dedupe']:
                pkey_list = list(job['PrimaryKey'].split(','))

                # first, count number of dupe rows for logging
                dupecount = df.duplicated(subset=pkey_list, keep='first').sum()

                dedupe_flag = job['Dedupe']
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
            else:
                existing_resource_desc = resource['description']

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
                        errmsg = 'TargetOrg "%s" does not exist!' % job['TargetOrg']
                        logecho(errmsg, level='error')
                        sys.exit(errmsg)

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
                existing_resource_desc = ''
                resources = package.get('resources')
                for resource in resources:
                    if resource['name'] == job['TargetResource']:
                        resource_exists = True
                        existing_resource_desc = resource['description']
                        resource_id = resource['id']
                        break

                #resource_id = ''
                if package and resource_exists:

                    if job['Truncate']:
                        try:
                            result = portal.action.datastore_delete(
                                resource_id=resource['id'],
                                force=True
                            )
                        except Exception as e:
                            logecho('    Truncate failed',
                                    level='error')
                            inputfile_error = True
                            inputfile_errordetails = str(e)

                    logecho('    "%s" (%s) exists in package "%s". Doing datastore_upsert...' % (
                        job['TargetResource'], resource['id'], job['TargetPackage']))
                    try:
                        result = portal.action.datastore_upsert(
                            force=True,
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
                        #resource_id = result['resource_id']
                        #resource_id = resource['id']
                else:
                    logecho('    "%s" does not exist in package "%s". Doing datastore_create...' % (
                        job['TargetResource'], job['TargetPackage']))

                    alias = '%s-%s-%s' % (job['TargetOrg'],
                                          job['TargetPackage'], job['TargetResource'])
                    resource = {
                        "package_id": package['id'],
                        "format": "csv",
                        "name": job['TargetResource']
                    }
                    try:
                        resource = portal.action.datastore_create(
                            force=True,
                            resource=resource,
                            aliases='',
                            fields=fields_dictlist,
                            records=data_dict,
                            primary_key=job['PrimaryKey'],
                            indexes=job['PrimaryKey'],
                            calculate_record_count=False
                        )
                    except Exception as e:
                        logecho('    Cannot create resource "%s"!' %
                                job['TargetResource'], level='error')
                        inputfile_error = True
                        inputfile_errordetails = str(e)
                    else:
                        logecho('    Created resource "%s"...' %
                                job['TargetResource'])
                        resource_id = resource['resource_id']
                        resource = portal.action.datastore_create(
                            force=True,
                            resource_id=resource_id,
                            aliases=alias,
                            calculate_record_count=True
                        )

            logecho('EXISTING DESC for resource %s: %s' %
                    (resource_id, existing_resource_desc), level='debug')
            updated_desc = ''
            if existing_resource_desc:
                result = re.split(r' \(UPDATED: (.*?)\)$',
                                  existing_resource_desc)
                if len(result) == 3:
                    # there is an old update date
                    updated_desc = result[0]
                else:
                    updated_desc = existing_resource_desc
            updated_desc = updated_desc + ' (UPDATED: %s)' % starttime
            logecho('RESOURCE UPDATED DESC: %s: %s' %
                    (resource_id, updated_desc), level='debug')
            portal.action.resource_update(
                id=resource_id,
                description=updated_desc)

            logecho('RESOURCE: %s' % resource, level='debug')

            if job['Stats'] and resource_id:
                logecho('    Computing stats...')
                result = computestats(
                    job['Stats'], job['PrimaryKey'], package, resource_id, job['TargetResource'],
                    starttime)

            t1_stop = perf_counter()
            t1_stopdt = datetime.now()
            endtime = t1_stopdt.strftime('%Y-%m-%d %H:%M:%S')
            elapsed = t1_stop - t1_start

            if inputfile_error:
                # inputfile processing failed, move to problemsdir
                try:
                    shutil.move(inputfile, problemsdir + '/' +
                                os.path.basename(inputfile))
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
                    shutil.move(inputfile, processeddir + '/' +
                                os.path.basename(inputfile))
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
    logecho('Starting datapump/%s...' % version)

    # log into CKAN
    try:
        portal = RemoteCKAN(host, apikey=apikey,
                            user_agent='datapump/' + version)
    except:
        logecho('Cannot connect to host %s' %
                host, level='error')
        sys.exit()
    else:
        logecho('Connected to host %s' % host)

    org_list = portal.action.organization_list()

    # process jobs
    jobs = os.scandir(inputdir)
    for job in jobs:
        if (not job.name.startswith('.') and job.name.endswith('-job.json') and
                job.is_file()):
            logecho('  Reading job - %s' % job)
            job_logger.info('Reading job - %s' % job)
            jobdefn = readjob(job)
            if jobdefn:
                logecho(json.dumps(jobdefn), level='debug')
                runjob(jobdefn)
                job_logger.info('%s executed' % job)
            else:
                logecho('  Invalid job json', level='error')
                job_logger.error('%s invalid' % job)

    logecho('Ending datapump...')


if __name__ == '__main__':
    datapump(auto_envvar_prefix='DATAPUMP')
