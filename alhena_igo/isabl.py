import alhenaloader
import os
import isabl_cli as ii
import logging
import pandas as pd

from scgenome.loaders.qc import load_qc_results


APP_VERSION = '1.0.0'
VERSION = "0.0.1"


# def clean(aliquot_id, host, port, projects=None):
#     analysis_id = get_id(aliquot_id)

#     es = alhenaloader.ES(host, port)

#     alhenaloader.clean_data(analysis_id, es)

#     es.delete_record_by_id(
#         es.ANALYSIS_ENTRY_INDEX, analysis_id)

#     es.remove_analysis_from_projects(analysis_id, projects=projects)


# def load(aliquot_id, host, port, projects, verbose=False):
#     if verbose:
#         logger = logging.getLogger('alhena')
#         logger.setLevel(logging.INFO)

#     [alignment, hmmcopy, annotation] = get_directories(aliquot_id)

#     analysis_id = get_id(aliquot_id)

#     metadata = get_metadata(analysis_id)

#     print(f'Loading as ID {analysis_id}')

#     data = load_qc_results(alignment, hmmcopy, annotation)

#     es = alhenaloader.ES(host, port)

#     alhenaloader.load_data(data, analysis_id, es)
#     es.load_record(
#         metadata, analysis_id, es.ANALYSIS_ENTRY_INDEX)
#     es.add_analysis_to_projects(analysis_id, projects)


def get_directories(target_aliquot: str):
    """Return alignment, hmmcopy, and annotation directory paths based off aliquot ID"""

    experiment = ii.get_experiments(aliquot_id=target_aliquot)[0]
    alignment = get_analysis('MONDRIAN-ALIGNMENT', VERSION, experiment.system_id)
    hmmcopy = get_analysis('MONDRIAN-HMMCOPY', VERSION, experiment.system_id)

    return [alignment["storage_url"], hmmcopy["storage_url"]]


def get_metadata(pk: str):
    """Return metadata object given target aliquot ID"""

    analysis = ii.get_instance("analyses", int(pk))
    data = analysis["targets"][0]

    dashboard_id = pk
    library_id = data["library_id"]
    sample_id = data["sample"]["identifier"]
    description = data["aliquot_id"]

    additional_metadata = {}

    metadata_record = alhenaloader.process_analysis_entry(dashboard_id, library_id, sample_id, description, additional_metadata)
    return metadata_record
    

def aliquot_to_pk(aliquot_id):
    experiment = ii.get_experiments(aliquot_id=aliquot_id)[0]
    hmmcopy = get_analysis('MONDRIAN-HMMCOPY', VERSION, experiment.system_id)

    return str(hmmcopy.pk)


def get_analysis(app, version, exp_system_id):
    """Return analysis record corresponding to system ID"""

    analyses = ii.get_analyses(
        application__name=app,
        application__version=version,
        targets__system_id=exp_system_id
    )
    if len(analyses) == 1:
        return analyses[0]

    return None


def get_ids_from_isabl(project_pk):
    experiments = ii.get_experiments(
        projects__pk=project_pk,
        technique__name='Single Cell DNA Seq',
    )
    data = []
    for experiment in experiments:
        hmmcopy = get_analysis("MONDRIAN-HMMCOPY", VERSION, experiment.system_id)
        if hmmcopy is not None:
            data.append({
                'system_id': experiment.system_id,
                'sample': experiment.sample.identifier,
                'aliquot': experiment.aliquot_id,
                "dashboard_id": str(hmmcopy.pk)
            })
    
    return data

