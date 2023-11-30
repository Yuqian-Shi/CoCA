"""using STIX 2.1 as default format for now"""
# import stix2validator
# import stix2
import uuid
import pyarrow


def json2recordbatch(obj):
    '''
    warp obj with objects, the input cloud be a single object or a list of objects
    '''
    temp = {'objects': []}
    if isinstance(obj, list):
        temp['objects'] = obj
    else:
        temp['objects'] = [obj]
    return pyarrow.RecordBatch.from_arrays(
        [temp['objects']], names=['objects'])


def recordbatchchunk2json(obj):
    data = obj.data.to_pydict()
    return data['objects']
