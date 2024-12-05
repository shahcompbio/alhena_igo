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
import pandas as pd
import numpy as np

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
@click.option('--analysis', help="Analysis ID", required=True)
@pass_info
def clean(info: Info, analysis: str):
    """Delete indices/records associated with analysis ID"""
    alhenaloader.clean_analysis(analysis, info.es)



@cli.command()
@click.option('--analysis_id', help="MONDRIAN-HMMCOPY or SCDNA-ANNOTATION analysis primary key", required=True)
@click.option('--project', 'projects', multiple=True, default=["DLP"], help="Projects to load analysis into")
@click.option('--framework', type=click.Choice(['scp', 'mondrian', 'mondrian_nf']), help="Framework: scp, mondrian or mondrian_nf (nextflow)")
@click.option('--version', help="Isabl app version to load", required=True)
@pass_info
def load(info: Info, analysis_id: str, projects: List[str], framework: str, version: str):
    click.echo(f'Loading as ID {analysis_id}')

    if framework == 'scp':
        [alignment, hmmcopy, annotation] = alhena_igo.isabl.get_directories(analysis_id, framework, version)
        data = load_qc_results(
            'scp', 
            alignment_results_dir=alignment, 
            hmmcopy_results_dir=hmmcopy,
            annotation_results_dir=annotation
        )
    elif framework == 'mondrian':
        [alignment, hmmcopy] = alhena_igo.isabl.get_directories(analysis_id, framework, version)
        data = load_qc_results(
            'mondrian', 
            alignment_results_dir=alignment, 
            hmmcopy_results_dir=hmmcopy
        )
    elif framework == 'mondrian_nf':
        [qc] = alhena_igo.isabl.get_directories(analysis_id, framework, version)
        data = load_qc_results(
            'mondrian_nf', 
            qc_results_dir=qc
        )
    else:
        raise Exception(f"Unknown framework option '{framework}'")

    metadata = alhena_igo.isabl.get_metadata(analysis_id)

    alhenaloader.load_analysis(analysis_id, data,  metadata, list(projects), info.es, framework)


@cli.command()
@click.option('--alhena', 'alhena', help='Projects to load into', multiple=True, default=[])
@click.option('--isabl', help="Project PK from Isabl to pull from", required=True)
@click.option('--framework', type=click.Choice(['scp', 'mondrian']), help="Framework: scp or mondrian", required=True)
@click.option('--version', help="Isabl app version to load", required=True)
@pass_info
def load_project(info: Info, alhena: List[str], isabl: str, framework:str, version: str):
    projects = list(set(list(alhena) + ["DLP"]))

    isabl_records = alhena_igo.isabl.get_ids_from_isabl(isabl, framework, version)
    isabl_pks = [record['dashboard_id'] for record in isabl_records]
    alhena_analyses = [record['dashboard_id'] for record in info.es.get_analyses()]

    diff = list(set(isabl_pks) - set(alhena_analyses))

    for analysis_id in diff: 
        alhenaloader.clean_analysis(analysis_id, info.es)

        click.echo(f'Loading as ID {analysis_id}')

        if framework == 'mondrian':
            [alignment, hmmcopy] = alhena_igo.isabl.get_directories(analysis_id, framework, version)
            data = load_qc_results(alignment, hmmcopy)
        elif framework == 'scp':
            [alignment, hmmcopy, annotation] = alhena_igo.isabl.get_directories(analysis_id, framework, version)
            data = load_qc_results(alignment, hmmcopy, annotation)
        else:
            raise Exception(f"Unknown framework '{framework}'.")

        metadata = alhena_igo.isabl.get_metadata(analysis_id)

        alhenaloader.load_analysis(analysis_id, data,  metadata, list(projects), info.es, framework)

    for project in projects:
        info.es.add_analyses_to_project(project, isabl_pks)


## Copied from wgs_analysis

def aggregate_adjacent(cnv, value_cols=(), stable_cols=(), length_normalized_cols=(), summed_cols=()):
    """ Aggregate adjacent segments with similar copy number state.

    see: https://github.com/amcpherson/remixt/blob/master/remixt/segalg.py

    Args:
        cnv (pandas.DataFrame): copy number table

    KwArgs:
        value_cols (list): list of columns to compare for equivalent copy number state
        stable_cols (list): columns for which values are the same between equivalent states
        length_normalized_cols (list): columns that are width normalized for equivalent states

    Returns:
        pandas.DataFrame: copy number with adjacent segments aggregated
    """

    # Group segments with same state
    cnv = cnv.sort_values(['patient', 'cell_id', 'chr', 'start'])
    cnv['chromosome_index'] = np.searchsorted(np.unique(cnv['chr']), cnv['chr'])
    cnv['diff'] = cnv[['chromosome_index'] + value_cols].diff().abs().sum(axis=1)
    cnv['is_diff'] = (cnv['diff'] != 0)
    cnv['cn_group'] = cnv['is_diff'].cumsum()

    def agg_segments(df):
        a = df[stable_cols].iloc[0]

        a['patient'] = df['patient'].min()
        a['cell_id'] = df['cell_id'].min()
        a['chr'] = df['chr'].min()
        a['start'] = df['start'].min()
        a['end'] = df['end'].max()
        a['length'] = df['length'].sum()

        for col in length_normalized_cols:
            a[col] = (df[col] * df['length']).sum() / (df['length'].sum() + 1e-16)

        for col in summed_cols:
            a[col] = df[col].sum()

        return a

    aggregated = cnv.groupby('cn_group').apply(agg_segments)

    for col in aggregated:
        aggregated[col] = aggregated[col].astype(cnv[col].dtype)

    return aggregated


@cli.command()
@click.argument('dashboard_id')
@click.argument('sample_id')
@click.argument('library_id')
@click.argument('description')
@click.argument('signals_filename')
@click.argument('metrics_filename')
@click.option('--project', '-p', 'projects', multiple=True, default=["DLP"], help="Projects to load analysis into")
@pass_info
def load_signals(info: Info, dashboard_id: str, sample_id: str, library_id: str, description: str, signals_filename: str, metrics_filename: str, projects: List[str]):

    metadata = {
        "dashboard_id": dashboard_id,
        "jira_id": dashboard_id,
        "sample_id": sample_id,
        "library_id": library_id,
        "description": description,
    }

    click.echo(f'Loading as ID {dashboard_id}')

    signals = pd.read_csv(
        signals_filename,
        dtype={
            'patient': 'str',
            'cell_id': 'str',
            'chr': 'str',
            'start': 'int64',
            'end': 'int64',
            'state': 'int64',
            'copy': 'float64',
            'state_AS_phased': 'str',
            'alleleA': 'float64',
            'alleleB': 'float64',
            'totalcounts': 'float64',
            'BAF': 'float64',
            'state_min': 'int64',
            'Maj': 'int64',
            'Min': 'int64',
            'LOH': 'str',
            'phase': 'str',
            'state_phase': 'str',
            'state_BAF': 'float64',
        })

    metrics = pd.read_csv(
        metrics_filename,
        dtype={
            'cell_id': 'str',
        })

    # Subset metrics by signals cells
    metrics = metrics.merge(signals[['cell_id']].drop_duplicates())

    empty_gc_metrics = pd.DataFrame()

    signals['length'] = signals['end'] - signals['start'] + 1

    signals_segs = aggregate_adjacent(
        signals,
        value_cols=['Maj', 'Min'],
        stable_cols=['state', 'state_min'],
        length_normalized_cols=['copy'],
        summed_cols=['alleleA', 'alleleB', 'totalcounts'])

    signals_segs['BAF'] = signals_segs['alleleB'] / signals_segs['totalcounts']

    data = {
        'hmmcopy_metrics': metrics,
        'hmmcopy_segs': signals_segs,
        'hmmcopy_reads': signals,
        'gc_metrics': empty_gc_metrics,
    }

    alhenaloader.load_data(data, dashboard_id, info.es, framework='mondrian')

    info.es.load_record(
        metadata, dashboard_id, info.es.ANALYSIS_ENTRY_INDEX)

    info.es.add_analysis_to_projects(dashboard_id, list(projects))


@cli.command()
def version():
    """Get the library version."""
    click.echo(click.style(f"{__version__}", bold=True))


