import logging
from colorama import Fore, Back, Style
import click
import click_config_file
from ckanapi import RemoteCKAN
import pandas as pd
import json
import os

# global variables
gua = 'datapump/1.0'
gloglevel = 'info'

logging.basicConfig(
    filename='./datapump.log',
    format='%(asctime)s - %(levelname)s - %(message)s',
    force=True,
    level=logging.INFO
)

# helper for logging to file and console
def logecho(message, level='info', verbose=False):
    message = level.upper() + ': ' + message
    if level == 'error':
        logging.error(message)
        click.echo(Fore.RED + message, err=True) if verbose else False
    elif level == 'warning':
        logging.warning(message)
        click.echo(Fore.YELLOW + message) if verbose else False
    elif level == 'debug':
        logging.debug(message)
        click.echo(Fore.GREEN + message) if verbose else False
    else:
        logging.info(message)
        click.echo(message) if verbose else False


def readjob(job):
    with open(job) as f:
        jobdefn = json.load(f)

    return jobdefn


@click.command()
@click.option('--inputdir',
              type=click.Path(exists=True),
              default='./input/',
              show_default=True,
              help='The directory where the job files are located.')
@click.option('--processeddir',
              type=click.Path(exists=True),
              default='./processed/',
              show_default=True,
              help='The directory where successfully processed job '
              'files are moved.')
@click.option('--problemsdir',
              type=click.Path(exists=True),
              default='./problems/',
              show_default=True,
              help='The directory where job that were not successfully '
              'processed are moved.')
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
              help='Enable debugging.')
@click_config_file.configuration_option(config_file_name='datapump.ini')
def datapump(inputdir, processeddir, problemsdir, host, apikey, verbose,
             debug):
    """Pumps data into CKAN using a simple directory-based queueing system."""
    logging.info('Starting datapump...')

    if debug:
        logging.setLevel(logging.DEBUG)
        gloglevel = 'debug'
    else:
        gloglevel = 'info'

    # log into CKAN
    try:
        portal = RemoteCKAN(host, apikey=apikey, user_agent=gua)
    except:
        logecho('Cannot connect to host %s' %
                host, level='error', verbose=verbose)
        sys.exit()
    else:
        logecho('Connected to host %s' % host, verbose=verbose)

    # read jobs
    jobs = os.scandir(inputdir)
    for job in jobs:
        if (not job.name.startswith('.') and job.name.endswith('-job.json') and
                job.is_file()):
            logecho('Reading job - %s' % job, verbose=verbose)
            jobdefn = readjob(job)
            logecho(json.dumps(jobdefn), level=gloglevel, verbose=verbose)

    logging.info('Ending datapump...')


if __name__ == '__main__':
    datapump(auto_envvar_prefix='DATAPUMP')
