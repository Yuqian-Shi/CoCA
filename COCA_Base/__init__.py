"""
Base function and models for COCA
Author: Shi Yuqian
Date: 2022/10/20

"""
import time
import uuid
import pytz
import datetime as dt
import json
import sys
import logging
import logging.handlers
import datetime
from json2parquet import ingest_data


def dt_to_str(dttm):
    """
    Given a datetime instance, produce the string representation
    with microsecond precision
    @param dttm: datetime instance
    @return: string representation of datetime instance
    """
    # 1. Convert to timezone-aware
    # 2. Convert to UTC
    # 3. Format in ISO format with microsecond precision

    if dttm.tzinfo is None or dttm.tzinfo.utcoffset(dttm) is None:
        # dttm is timezone-naive; assume UTC
        zoned = pytz.UTC.localize(dttm)
    else:
        zoned = dttm.astimezone(pytz.UTC)
    return zoned.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def str_to_dt(timestamp_string):
    """
    Convert string timestamp to datetime instance.
    @param timestamp_string: string representation of datetime instance
    @return: datetime instance
    """
    try:
        return dt.datetime.strptime(timestamp_string, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError:
        return dt.datetime.strptime(timestamp_string, "%Y-%m-%dT%H:%M:%SZ")


def get_now_str(shift_sec=0):
    """
    return current time stamp in ISO8601 format plus ending Z
    @param shift_sec: shift in seconds
    @return: string
    """
    dt = datetime.datetime.utcnow() + datetime.timedelta(seconds=shift_sec)
    return dt.isoformat() + "Z"


def load_dict(p):
    """
    load json file
    """
    f = open(p, 'r', encoding='utf-8')
    d = f.read()
    f.close()
    return json.loads(d)


def save_dict(d, p, ensure_ascii=True):
    """
    save json to file
    """
    save_file(p, json.dumps(d, sort_keys=True, indent=4,
                            ensure_ascii=ensure_ascii))


def if_list_have_duplicates(d):
    """
    Checks if any lists in a dictionary have duplicate elements.
    @param d: dictionary
    @return: True if any lists have duplicates, False otherwise
    """
    result = False
    for _, v in d.items():
        if type(v) is list:
            if len(v) != len(set(v)):
                return True, "duplicate elements found in {0}".format(v)
        if type(v) is dict:
            sub_result, sub_msg = if_list_have_duplicates(v)
            if sub_result:
                return True, sub_msg
    return False, ""


def if_required_keys_exists(keys, d):
    """
    check if required keys all exist in a dictionary
    raise ValueError if not
    @param keys: list of keys
    @param d: dictionary
    """
    for k in keys:
        if not k in d:
            raise ValueError("key [{0}] missing".format(k))


def json2column(j):
    '''
    Warning: ingest_data will use [j] instead of  j
    @param j: json
    '''
    return ingest_data([j])


def if_list_have_duplicates(d):
    '''
    Check if any lists in a dictionary have duplicate elements.
    @param d: dictionary
    '''
    result = False
    for _, v in d.items():
        if type(v) is list:
            if len(v) != len(set(v)):
                return True, "duplicate elements found in {0}".format(v)
        if type(v) is dict:
            sub_result, sub_msg = if_list_have_duplicates(v)
            if sub_result:
                return True, sub_msg
    return False, ""


def save_file(p, d, m='', t='w'):
    f = open(p, t)
    f.write(d)
    f.close()


def list_differences(a, b):
    result = []
    for e in a:
        if not e in b:
            result.append(e)
    for e in b:
        if not e in a:
            result.append(e)
    return result


def get_timestamp():
    """Get current time with UTC offset"""
    return dt.datetime.now(tz=pytz.UTC)


def get_modified(d):
    '''get modified,add modified as created if not exists'''
    if not 'modified' in d:
        d['modified'] = d['created']
    return d.get('modified')


def generate_status_details(id, version, message=None):
    """Generate Status Details as defined in TAXII 2.1 section (4.3.1) <link here>`__."""
    status_details = {
        "id": id,
        "version": version
    }

    if message:
        status_details["message"] = message

    return status_details


def generate_status(
    request_time, status, succeeded, failed, pending,
    successes=None, failures=None, pendings=None,
):
    """
    Generate Status Resource as defined in TAXII 2.1 section (4.3.1) <link here>`__.
    @param request_time: datetime instance
    @param status: status string
    @param succeeded: number of succeeded
    @param failed: number of failed
    @param pending: number of pending
    @param successes: list of success
    @param failures: list of failures
    @param pendings: list of pendings
    """
    status = {
        "id": str(uuid.uuid4()),
        "status": status,
        "request_timestamp": request_time,
        "total_count": succeeded + failed + pending,
        "success_count": succeeded,
        "failure_count": failed,
        "pending_count": pending,
    }

    if successes:
        status["successes"] = successes
    if failures:
        status["failures"] = failures
    if pendings:
        status["pendings"] = pendings

    return status


def get_config_info_for_client(path):
    d = load_dict(path)
    print(d)


def determine_version(new_obj, request_time):
    """Grab the modified time if present, if not grab created time,
    if not grab request time provided by server.
    version is a string of time"""
    return new_obj.get("modified", new_obj.get("created", datetime_to_string(request_time)))


def determine_spec_version(obj):
    """Given a STIX 2.x object, determine its spec version."""
    missing = ("created", "modified")
    if all(x not in obj for x in missing):
        # Special case: only SCOs are 2.1 objects and they don't have a spec_version
        # For now the only way to identify one is checking the created and modified
        # are both missing.
        return "2.1"
    return obj.get("spec_version", "2.0")


def get_uuid():
    return str(uuid.uuid4())


def get_logger(level='info', test_flag=False,
               filename='',
               maxsize=1024*1024*3,
               backup_num=5, encoding='utf-8', print_to_cmd=None):
    '''
    returns a logger, 
    if test_flag is True or level is set to logging.DEBUG,
    it will output to terminal
    '''
    level = level.upper()
    if level == 'INFO':
        level = logging.INFO
    elif level == 'WARNING' or level == 'WARN':
        level = logging.WARNING
    elif level == 'ERROR' or level == 'ERR':
        level = logging.ERROR
    elif level == 'DEBUG':
        level = logging.DEBUG
    else:
        print('unknown logging level:{level}')
    if filename == '':
        temp = sys.argv[0].split('.')
        temp = temp[:-1]
        filename = '.'.join(temp) + '.log'

    logger = logging.getLogger()
    format = logging.Formatter(
        "\n%(levelname)s - line:%(lineno)d - %(filename)s = %(asctime)s\n[ %(message)s ]")    # output format

    handler = logging.handlers.RotatingFileHandler(
        filename, maxBytes=maxsize, backupCount=backup_num, encoding=encoding)
    handler.setFormatter(format)
    logger.addHandler(handler)
    if not print_to_cmd is None:
        if print_to_cmd:
            # output to standard output
            sh = logging.StreamHandler(stream=sys.stdout)
            sh.setFormatter(format)
            logger.addHandler(sh)
    elif level == logging.DEBUG:
        sh = logging.StreamHandler(stream=sys.stdout)
        sh.setFormatter(format)
        logger.addHandler(sh)
    logger.setLevel(level)
    return logger


def time_this(fn):
    def wrap(*args, **kwargs):
        start = time.time()
        fn(*args, **kwargs)
        print('[{0}] took {1:.10f} seconds'.format(
            fn.__name__, time.time() - start))
    return wrap


def get_hostname():
    import socket
    return socket.gethostname()
