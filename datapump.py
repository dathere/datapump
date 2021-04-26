import logging
from colorama import Fore, Back, Style
import click
import click_config_file
from ckanapi import RemoteCKAN
import pandas as pd


ua = 'datapump/1.0'

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
@click_config_file.configuration_option(config_file_name='datapump.ini')
def datapump(inputdir, processeddir, problemsdir, host, apikey, verbose):
    """Pumps data into CKAN using a simple directory-based queueing system."""
    logging.info('Starting datapump...')

    try:
        portal = RemoteCKAN(host, apikey=apikey, user_agent=ua)
    except:
        logecho('Cannot connect to host %s' %
                host, level='error', verbose=verbose)
        sys.exit()
    else:
        logecho('Connected to host %s' % host, verbose=verbose)
    groups = portal.action.group_list()

    logging.info('Ending datapump...')


if __name__ == '__main__':
    datapump(auto_envvar_prefix='DATAPUMP')
