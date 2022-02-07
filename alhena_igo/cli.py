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
from scgenome.loaders.qc import load_qc_results

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
@click.option('--analysis', help="Analysis ID")
@pass_info
def clean(info: Info, id: str, analysis: str):
    """Delete indices/records associated with analysis ID"""
    assert id is not None or analysis is not None,  "Please specify either aliquot or analysis ID"

    analysis_id = alhena_igo.isabl.aliquot_to_pk(id) if id is not None else analysis

    alhenaloader.clean_analysis(analysis_id, info.es)



@cli.command()
@click.option('--id', help="Aliquot ID", required=True)
@click.option('--project', '-p', 'projects', multiple=True, default=["DLP"], help="Projects to load analysis into")
@click.option('--framework', '-f', type=click.Choice(['scp', 'mondrian']), help="Framework: scp or mondrian")
@pass_info
def load(info: Info, id: str, projects: List[str], framework: str):

    click.echo(f'Loading as ID {analysis_id}')

    if framework == 'scp':
        [alignment, hmmcopy, annotation] = alhena_igo.isabl.get_directories(id, framework)
        data = load_qc_results(alignment, hmmcopy, annotation)
    elif framework == 'mondrian':
        [alignment, hmmcopy] = alhena_igo.isabl.get_directories(id, framework)
        data = load_qc_results(alignment, hmmcopy)
    else:
        raise Exception(f"Unknown framework option '{framework}'")

    analysis_id = alhena_igo.isabl.get_id(id, framework)

    metadata = alhena_igo.isabl.get_metadata(analysis_id)

    alhenaloader.load_analysis(analysis_id, data,  metadata, list(projects), info.es, framework)


@cli.command()
@click.option('--alhena', 'alhena', help='Projects to load into', multiple=True, default=[])
@click.option('--isabl', help="Project PK from Isabl to pull from", required=True)
@click.option('--framework', '-f', type=click.Choice(['scp', 'mondrian']), help="Framework: scp or mondrian")
@pass_info
def load_project(info: Info, alhena: List[str], isabl: str, framework:str):
    projects = list(set(list(alhena) + ["DLP"]))

    isabl_records = alhena_igo.isabl.get_ids_from_isabl(isabl, framework)
    isabl_pks = [record['dashboard_id'] for record in isabl_records]
    alhena_analyses = [record['dashboard_id'] for record in info.es.get_analyses()]

    diff = list(set(isabl_pks) - set(alhena_analyses))

    for analysis_id in diff: 
        alhenaloader.clean_analysis(analysis_id, info.es)

        aliquot_id = [record['aliquot'] for record in isabl_records if record['dashboard_id'] == analysis_id][0]

        click.echo(f'Loading as ID {analysis_id}')

        if framework == 'mondrian':
            [alignment, hmmcopy] = alhena_igo.isabl.get_directories(aliquot_id, framework)
            data = load_qc_results(alignment, hmmcopy)
        elif framework == 'scp':
            [alignment, hmmcopy, annotation] = alhena_igo.isabl.get_directories(aliquot_id, framework)
            data = load_qc_results(alignment, hmmcopy, annotation)
        else:
            raise Exception(f"Unknown framework '{framework}'.")

        metadata = alhena_igo.isabl.get_metadata(analysis_id)

        alhenaloader.load_analysis(analysis_id, data,  metadata, list(projects), info.es, framework)

    for project in projects:
        info.es.add_analyses_to_project(project, isabl_pks)


@cli.command()
def version():
    """Get the library version."""
    click.echo(click.style(f"{__version__}", bold=True))
