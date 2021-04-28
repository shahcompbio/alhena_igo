import alhenaloader
import os
import isabl_cli as ii


APP_VERSION = '1.0.0'
os.environ["ISABL_API_URL"] = 'https://isabl.shahlab.mskcc.org/api/v1/'
os.environ['ISABL_CLIENT_ID'] = '1'
VERSION = "0.0.1"


def load(aliquot_id, host, port, views):
    [alignment, hmmcopy, annotation] = get_directories(id)

    dashboard_id = get_id(id)

    metadata = get_metadata(dashboard_id)

    print(f'Loading as ID {dashboard_id}')

    data = alhenaloader.load_qc_from_dirs(alignment, hmmcopy, annotation)
    alhenaloader.load_data(data, dashboard_id, info.es)

    es = alhenaloader.ES(host, port)

    es.load_record(
        metadata, dashboard_id, es.DASHBOARD_ENTRY_INDEX)

    es.add_dashboard_to_views(dashboard_id, list(views))


def get_directories(target_aliquot: str):
    """Return alignment, hmmcopy, and annotation directory paths based off aliquot ID"""

    experiment = ii.get_instances("experiments", aliquot_id=target_aliquot)[0]

    alignment = get_analyses('SCDNA-ALIGNMENT', VERSION, experiment.system_id)
    hmmcopy = get_analyses('SCDNA-HMMCOPY', VERSION, experiment.system_id)
    annotation = get_analyses(
        'SCDNA-ANNOTATION', VERSION, experiment.system_id)

    return [alignment["storage_url"], hmmcopy["storage_url"], annotation["storage_url"]]


def get_id(target_aliquot: str):
    experiment = ii.get_instances("experiments", aliquot_id=target_aliquot)[0]
    alignment = get_analyses('SCDNA-ALIGNMENT', VERSION, experiment.system_id)

    return str(alignment.pk)


def get_metadata(pk: str):
    """Return metadata object given target aliquot ID"""

    analysis = ii.get_instance("analyses", int(pk))
    data = analysis["targets"][0]

    return {
        "dashboard_id": pk,
        "jira_id": pk,
        "sample_id": data["sample"]["identifier"],
        "library_id": data["library_id"],
        "description": data["aliquot_id"]
    }


def get_analyses(app, version, exp_system_id):
    """Return analysis record corresponding to system ID"""

    analyses = ii.get_instances(
        'analyses',
        application__name=app,
        application__version=version,
        targets__system_id=exp_system_id
    )
    assert len(analyses) == 1
    return analyses[0]
