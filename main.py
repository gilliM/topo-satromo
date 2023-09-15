import click
import os
os.environ['SIMPLE_SETTINGS'] = 'configuration.settings'
from simple_settings import settings
from misc.logger import logger
from processors import Processor
from satromo_publish import Publisher


@click.group()
@click.option('--debug/--no-debug', default=False)
@click.option('--config', 'config_file', default=None)
def cli(debug, config_file):
    if debug:
        settings.configure(log_level='DEBUG')

        # Set the CPL_DEBUG environment variable to enable verbose output fro GDAL
        os.environ["CPL_DEBUG"] = "ON"

    if config_file is not None:
        settings.configure(config_file=config_file)
    logger.debug(f"Debug mode is {'on' if debug else 'off'}")
    logger.info('base settings from {}'.format(os.environ['SIMPLE_SETTINGS']))

    from misc.run_utilities import set_config
    set_config()


@cli.command()
@click.option('-f', '--force', is_flag=True)
def process(force):
    logger.info('start processing')
    settings.configure(force_processing=force)
    p = Processor()
    p.run()


@cli.command()
@click.option('--skip_s3', is_flag=True, help='dev option to skip the step to copy files S3')
def publish(skip_s3):
    logger.info('start publishing')
    settings.configure(skip_s3=skip_s3)
    p = Publisher()
    p.run()


if __name__ == '__main__':
    cli()




