# -*- coding:utf-8 -*-
# Author: Yuqian SHi
# Date: 2022-10-21
"""
using apache flight to send STIX data
"""
import pyarrow as pa
import pyarrow.flight
import pyarrow.parquet
import json

import COCA_Base
import time
import threading


def get_client(addr, usr, pwd):
    """
    @param addr: address of the server
    @param usr: username
    @param pwd: password
    @return c: client instance
    @return t: token acquired from server
    get client instance and token from server
    """
    c = pa.flight.connect(addr)
    t = c.authenticate_basic_token(usr, pwd)
    return c, t


def upload_data(data_batch, c, t, location):
    '''
    This function uploads data to a specified location using Apache Arrow Flight.

    @param data_batch: The data to be uploaded.
    @param c: The client instance.
    @param t: The token acquired from the server.
    @param location: The API endpoint or collection where the data should be uploaded.
    @return: True if the upload was successful, False otherwise.
    '''
    upload_descriptor = pa.flight.FlightDescriptor.for_path(
        location)

    options = pa.flight.FlightCallOptions(headers=[t])
    print(data_batch.nbytes)
    try:
        writer, _ = c.do_put(descriptor=upload_descriptor,
                             schema=data_batch.schema, options=options)
        with writer:
            writer.write_batch(data_batch)
    except pa.flight.FlightUnauthenticatedError as e:
        print('UnauthenticatedError, please check your username and password / token')
        return False
    print('{0} uploaded'.format(len(data_batch)))
    return True


def list_flights(c, t):
    '''
    This function lists all the flights available on the server.

    @param c: The client instance.
    @param t: The token acquired from the server.
    @return: None. The function prints the details of each flight.

    The function prints the following details for each flight:
    - Path or Command based on the descriptor type
    - Total records
    - Total bytes
    - Number of endpoints
    '''
    options = pa.flight.FlightCallOptions(headers=[t])
    print('Flights\n=======')
    for flight in c.list_flights(options=options):
        descriptor = flight.descriptor
        if descriptor.descriptor_type == pyarrow.flight.DescriptorType.PATH:
            print("Path:", descriptor.path)
        elif descriptor.descriptor_type == pyarrow.flight.DescriptorType.CMD:
            print("Command:", descriptor.command)
        else:
            print("Unknown descriptor type")

        print("Total records:", end=" ")
        if flight.total_records >= 0:
            print(flight.total_records)
        else:
            print("Unknown")

        print("Total bytes:", end=" ")
        if flight.total_bytes >= 0:
            print(flight.total_bytes)
        else:
            print("Unknown")

        print("Number of endpoints:", len(flight.endpoints))
        print("Schema:")
        print(flight.schema)
        print('---')

    print('\nActions\n=======')
    for action in c.list_actions(options=options):
        print("Type:", action.type)
        print("Description:", action.description)
        print('---')


def do_action(c, t, action_name):
    '''
    This function performs a specific action on the server.

    @param c: The client instance.
    @param t: The token acquired from the server.
    @param action_name: The name of the action to be performed.
    @return: The result of the action if successful, otherwise prints an error message.

    The function sends an action request to the server and collects the results.
    If the action is not successful, it prints an error message.
    '''
    try:
        buf = pyarrow.allocate_buffer(0)
        action = pyarrow.flight.Action(
            action_name, buf,)
        print('Running action {0}'.format(action_name))
        re = []
        h = [t]
        for result in c.do_action(
                action, options=pa.flight.FlightCallOptions(headers=h, timeout=1)):
            re.append(result)
        b = re[0].body.to_pybytes()
        if b.decode('utf-8') == action_name:
            return re[1:]
        else:
            print('wrong response')
            print(b.decode('utf-8'))
            print(action_name)
    except pyarrow.flight.FlightTimedOutError as e:
        print("Error calling action:", e)


def do_shutdown(c, t):
    '''
    This function performs the 'shutdown' action on the server.

    @param c: The client instance.
    @param t: The token acquired from the server.
    @return: The result of the 'shutdown' action if successful.

    The function calls the do_action function with 'shutdown' as the action_name.
    '''
    return do_action(c, t, 'shutdown')


def exchange_data(c, t, location):
    '''
    This function exchanges data with the server.

    @param c: The client instance.
    @param t: The token acquired from the server.
    @param location: The API endpoint or collection where the data should be exchanged.
    @return: The result of the data exchange.

    The function sends a data exchange request to the server and returns the result.
    '''
    options = pa.flight.FlightCallOptions(headers=[t])
    return c.do_exchange(
        pa.flight.FlightDescriptor.for_path(location), options=options)


def do_discovery(c, t):
    '''
    This function performs the 'discovery' action on the server.

    @param c: The client instance.
    @param t: The token acquired from the server.
    @return: The result of the 'discovery' action if successful, otherwise prints an error message.

    The function calls the do_action function with 'discovery' as the action_name and returns the result.
    '''
    # should return only one object
    result = do_action(c, t, 'discovery')[0]
    result = result.body.to_pybytes()
    result = result.decode('utf-8')
    result = json.loads(result)
    return result


def load_data(p):
    '''
    Loads data from a JSON file and converts it to a RecordBatch.
    @param p: The path to the JSON file.
    @return: The RecordBatch.
    '''
    a = json.loads(open(p).read())
    k = list(a.keys())[0]
    c = pa.RecordBatch.from_arrays([a[k]], names=[k])
    return c
    # return j2q_load_json(p)


def write_every(writer, t=5, data=[]):
    '''
    Writes data to a writer every t seconds.
    @param writer: The writer instance.
    @param t: The time interval between writes.
    @param data: The data to be written.
    @return: None.
    '''
    data = list(map(lambda x: COCA_Base.json2column(x), data))
    sample = data[0]
    writer.begin(sample.schema)

    for d in data:
        print(d.schema)
        writer.write(d)
        print(time.time())
        time.sleep(5)
    writer.done_writing()


def read_print(reader):
    '''
    print the information of the object in reader
    @param reader: The reader instance.
    '''
    for d in reader:
        print(dir(d))


if __name__ == '__main__':
    d = json.loads(open('json_dumps.json').read())
    d = d['objects']
    c, t = get_client("grpc://192.168.1.92:8815",
                      'admin1', 'pwd_for_admin1')
    writer, reader = exchange_data(c, t, 'api1/observations')

    a = threading.Thread(write_every(writer, 5, d[:10]))
    a.setDaemon(True)
    a.start()
    read_print(reader)
