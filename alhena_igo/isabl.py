from scgenome.loaders.qc import load_qc_results
import isabl_cli as ii
import pandas as pd
import alhenaloader
import logging
import os


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


def get_directories(target_aliquot: str, framework: str, version: str):
    """Return alignment, hmmcopy, and annotation directory paths based off aliquot ID"""
    experiment = ii.get_experiments(aliquot_id=target_aliquot)[0]
 
    if framework == 'mondrian':
        alignment = get_analysis('MONDRIAN-ALIGNMENT', version, experiment.system_id)
        hmmcopy = get_analysis('MONDRIAN-HMMCOPY', version, experiment.system_id)
        return [alignment["storage_url"], hmmcopy["storage_url"]]
    elif framework == 'scp':
        alignment = get_analysis('SCDNA-ALIGNMENT', version, experiment.system_id)
        hmmcopy = get_analysis('SCDNA-HMMCOPY', version, experiment.system_id)
        annotation = get_analysis('SCDNA-ANNOTATION', version, experiment.system_id)
        return [alignment["storage_url"], hmmcopy["storage_url"], annotation["storage_url"]]
    else:
        raise Exception(f"Unknown framework '{framework}'")


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


def get_analysis(app, version, exp_system_id):
    """Return analysis record corresponding to system ID"""
    analyses = ii.get_analyses(
        application__name=app,
        application__version=version,
        targets__system_id=exp_system_id,
        status='SUCCEEDED',
    )
    if len(analyses) == 1:
        return analyses[0]

    return None


def get_id(aliquot_id, framework, version):
    app = ''

    if framework == 'mondrian':
        app = 'MONDRIAN-HMMCOPY'
    elif framework == 'scp':
        app = 'SCDNA-ANNOTATION'
    else:
        raise Exception(f"Unknown framework '{framework}'.")

    analysis = ii.get_analyses(
        application__name=app,
        status='SUCCEEDED',
        targets__aliquot_id=aliquot_id,
        application__version=version,
    )
    assert len(analysis) == 1
    return str(analysis[0].pk)


def get_ids_from_isabl(project_pk, framework, version):
    experiments = ii.get_experiments(
        projects__pk=project_pk,
        technique__name='Single Cell DNA Seq',
    )

    app = ''
    if framework == 'mondrian':
        app = 'MONDRIAN-HMMCOPY'
    elif framework == 'scp':
        app = 'SCDNA-ANNOTATION'
    else:
        raise Exception(f"Unknown framework '{framework}'.")

    data = []
    for experiment in experiments:
        analysis = get_analysis(app, version, experiment.system_id)
        if analysis is not None:
            data.append({
                'system_id': experiment.system_id,
                'sample': experiment.sample.identifier,
                'aliquot': experiment.aliquot_id,
                "dashboard_id": str(analysis.pk),
            })
    
    return data

