import click
import yaml
from loguru import logger
import sys
from satromo_processor import Processor
from satromo_publish import Publisher


@click.group()
@click.option('--debug/--no-debug', default=False)
def cli(debug):
    logger.info(f"Debug mode is {'on' if debug else 'off'}")

@cli.command()  # @cli, not @click!
def process():
    logger.info('start processing')
    p = Processor()
    p.run()

@cli.command()  # @cli, not @click!
def publish():
    logger.info('publishing')


if __name__ == '__main__':
    cli()