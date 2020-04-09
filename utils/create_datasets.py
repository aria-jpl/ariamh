import json
from datetime import datetime


def create_dataset_json(dataset_id, version, start_time,
                        end_time=datetime.now().strftime('%Y-%m-%dT%H:%M:%S'), **kwargs):
    '''
    :param dataset_id: ex. S1-GUNW-MERGED_TN014_20190520T112233-20190410T112233-poeorb-HJ45
    :param version: str, ex. 'v2.0.0'
    :param start_time: str, start time of the job
    :param end_time: str: end time of the job, by default it will be datetime.now()
    :param kwargs: additional data you want to add
    :return: void
    '''

    metadata = {
        'label': dataset_id,
        'version': version,
        'starttime': start_time,
        'endtime': end_time,
        'creation_timestamp': datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    }

    for key, value in list(kwargs.items()):  # adding additional metadata specified in kwargs
        metadata[key] = value

    file_name = '{}.dataset.json'.format(dataset_id)
    with open(file_name, 'w') as f:
        metadata_json = json.dumps(metadata, indent=2)
        f.write(metadata_json)
    return True
