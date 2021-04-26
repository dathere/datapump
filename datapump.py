import logging
import click
from ckanapi import RemoteCKAN
import pandas as pd

ua = 'datapump/1.0'

fileh = logging.FileHandler('./datapump.log', 'a')
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
fileh.setFormatter(formatter)

logger = logging.getLogger()
logger.addHandler(fileh)

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
def datapump(inputdir, processeddir, problemsdir, host, apikey, verbose):
    """Pumps data into CKAN."""
    logger.info('Starting datapump...')

    try:
        portal = RemoteCKAN(host, user_agent=ua)
    except:
        logger.error('Cannot connect to host %s' % host)
        click.echo('Cannot connect to host %s' % host) if verbose else False
    else:
        click.echo('Connected to host %s' % host) if verbose else False
        logger.info('Connected to host %s' % host)
    groups = portal.action.group_list()

    logger.info('Ending datapump...')


if __name__ == '__main__':
    datapump(auto_envvar_prefix='DATAPUMP')
