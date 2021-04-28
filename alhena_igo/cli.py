#!/usr/bin/env python
# -*- coding: utf-8 -*-
import alhena_igo.isabl

"""
This is the entry point for the command-line interface (CLI) application.

It can be used as a handy facility for running the task from a command line.

.. note::

    To learn more about Click visit the
    `project website <http://click.pocoo.org/5/>`_.  There is also a very
    helpful `tutorial video <https://www.youtube.com/watch?v=kNke39OZ2k0>`_.

    To learn more about running Luigi, visit the Luigi project's
    `Read-The-Docs <http://luigi.readthedocs.io/en/stable/>`_ page.

.. currentmodule:: alhena_igo.cli
.. moduleauthor:: Samantha Leung <leungs1@mskcc.org>
"""
import logging
import click
from typing import List

import alhenaloader

import alhena_igo.isabl
from .__init__ import __version__

LOGGING_LEVELS = {
    0: logging.NOTSET,
    1: logging.ERROR,
    2: logging.WARN,
    3: logging.INFO,
    4: logging.DEBUG,
}  #: a mapping of `verbose` option counts to logging levels

LOGGING_FORMAT = "%(asctime)s - %(levelname)s - %(funcName)s - %(message)s"


class Info(object):
    """An information object to pass data between CLI functions."""

    def __init__(self):  # Note: This object must have an empty constructor.
        """Create a new instance."""
        self.verbose: int = 0


# pass_info is a decorator for functions that pass 'Info' objects.
#: pylint: disable=invalid-name
pass_info = click.make_pass_decorator(Info, ensure=True)


# Change the options to below to suit the actual options for your task (or
# tasks).
@click.group()
@click.option("--verbose", "-v", count=True, help="Enable verbose output.")
@click.option('--host', default='localhost', help='Hostname for Elasticsearch server')
@click.option('--port', default=9200, help='Port for Elasticsearch server')
@pass_info
def cli(info: Info, verbose: int, host: str, port: int):
    """Run alhena_igo."""
    # Use the verbosity count to determine the logging level...
    if verbose > 0:
        logging.basicConfig(format=LOGGING_FORMAT)
        alhenaLogger = logging.getLogger('alhena')
        igoLogger = logging.getLogger('alhena_igo')

        alhenaLogger.setLevel(LOGGING_LEVELS[verbose]
                              if verbose in LOGGING_LEVELS
                              else logging.DEBUG)
        igoLogger.setLevel(LOGGING_LEVELS[verbose]
                           if verbose in LOGGING_LEVELS
                           else logging.DEBUG)

        click.echo(
            click.style(
                f"Verbose logging is enabled. "
                f"(LEVEL={logging.getLogger().getEffectiveLevel()})",
                fg="yellow",
            )
        )
    info.verbose = verbose
    info.es = alhenaloader.ES(host, port)


@cli.command()
@click.option('--id', help="Aliquot ID")
@click.option('--dashboard', help="Dashboard ID")
@pass_info
def clean(info: Info, id: str, dashboard: str):
    """Delete indices/records associated with dashboard ID"""
    assert id is not None or dashboard is not None,  "Please specify either aliquot or dashboard ID"

    dashboard_id = alhena_igo.isabl.get_id(id) if id is not None else dashboard

    alhenaloader.clean_data(dashboard_id, info.es)

    info.es.delete_record_by_id(
        info.es.DASHBOARD_ENTRY_INDEX, dashboard_id)

    info.es.remove_dashboard_from_views(dashboard_id)


@cli.command()
@click.option('--id', help="Aliquot ID", required=True)
@click.option('--view', '-v', 'views', multiple=True, default=["DLP"], help="Views to load dashboard into")
@pass_info
def load(info: Info, id: str, views: List[str]):

    [alignment, hmmcopy, annotation] = alhena_igo.isabl.get_directories(id)

    dashboard_id = alhena_igo.isabl.get_id(id)

    metadata = alhena_igo.isabl.get_metadata(dashboard_id)

    click.echo(f'Loading as ID {dashboard_id}')

    data = alhenaloader.load_qc_from_dirs(alignment, hmmcopy, annotation)
    alhenaloader.load_data(data, dashboard_id, info.es)

    info.es.load_record(
        metadata, dashboard_id, info.es.DASHBOARD_ENTRY_INDEX)

    info.es.add_dashboard_to_views(dashboard_id, list(views))


@cli.command()
def version():
    """Get the library version."""
    click.echo(click.style(f"{__version__}", bold=True))
