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

    # helper for logging to file and console
    def logecho(message, level='info'):
        if level == 'error':
            logging.error(message)
            click.echo(Fore.RED + level.upper() + ': ' + Fore.WHITE +
                       message, err=True) if verbose else False
        elif level == 'warning':
            logging.warning(message)
            click.echo(Fore.YELLOW + level.upper() + ': ' +
                       Fore.WHITE + message) if verbose else False
        elif level == 'debug':
            logging.debug(message)
            click.echo(Fore.GREEN + level.upper() + ': ' +
                       Fore.WHITE + message) if debug else False
        else:
            logging.info(message)
            click.echo(message)

    ua = 'datapump/1.0'
    dateformats_list = dateformats.split(', ')

    logecho('DATEFORMATS: %s' % dateformats_list, level='debug')

    logging.basicConfig(
        filename=logfile,
        format='%(asctime)s - %(levelname)s - %(message)s',
        force=True,
        level=logging.INFO
    )
    logger = logging.getLogger('')
    org_list = []


    def _get_col_dtype(col):
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

    # helper for updating job json file
    def updatejob(job, jobinfo):
        with open(job, 'w') as f:
            json.dump(jobinfo, f, indent=4)

    # helper for run job
    def runjob(job):
        joberror = False
        joberrordetails = ''
        t1_startdt = datetime.now()
        t1_start = perf_counter()

        inputfiles = glob.glob(job['InputFile'])
        logecho('  %s file/s found for %s: ' %
                (len(inputfiles), job['InputFile']))

        # process files, order by most recent
        inputfiles.sort(key=os.path.getmtime, reverse=True)
        for inputfile in inputfiles:
            logecho('    Processing: %s...' % inputfile)

            def custom_date_parser(x): return dateparser.parse(
                x, date_formats=dateformats_list)

            df = pd.read_csv(inputfile, parse_dates=[
                             datecolumn], date_parser=custom_date_parser)

            colname_list = df.columns.tolist()

            coltype_list = []
            for column in df:
                coltype_list.append(_get_col_dtype(df[column]))

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
                        logecho('Cannot create package "%s"!' %
                                job['TargetPackage'], level='error')
                        joberror = True
                        joberrordetails = e
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
                resources = package['resources']
                for resource in resources:
                    if resource['name'] == job['TargetResource']:
                        resource_exists = True
                        break

                if resource_exists:
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
                        logecho('Upsert failed', level='error')
                        joberror = True
                        joberrordetails = e
                    else:
                        logecho('    Upsert successful!')
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
                        logecho('Cannot create resource "%s"!' %
                                job['TargetResource'], level='error')
                        joberror = True
                        joberrordetails = e
                    else:
                        logecho('    Created resource "%s"...' %
                                job['TargetResource'])

            logecho('RESOURCE: %s' % resource, level='debug')

        t1_stop = perf_counter()
        t1_stopdt = datetime.now()

        job["Processed"] = len(df.index) if 'df' in locals() else 0
        job["StartTime"] = t1_startdt.strftime('%Y-%m-%d %H:%M:%S')
        job["EndTime"] = t1_stopdt.strftime('%Y-%m-%d %H:%M:%S')
        job["Elapsed"] = t1_stop - t1_start
        if joberror:
            job["ReturnCode"] = joberrordetails.args
            job["ReturnCodeMsg"] = joberrordetails.message
        else:
            job["ReturnCode"] = 0
            job["ReturnCodeMsg"] = ''

        return job

    # datapump func main
    logecho('Starting datapump...')

    if debug:
        logger.setLevel(logging.DEBUG)

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
                jobinfo = runjob(jobdefn)

            updatejob(job, jobinfo)

            if jobinfo['ReturnCode'] == 0:
                # job was successfully processed, move to processeddir
                shutil.move(inputdir + '/' + job.name,
                            processeddir + '/' + job.name)
            else:
                # job failed, move to problemsdir
                shutil.move(inputdir + '/' + job.name,
                            problemsdir + '/' + job.name)

    logecho('Ending datapump...')


if __name__ == '__main__':
    datapump(auto_envvar_prefix='DATAPUMP')
