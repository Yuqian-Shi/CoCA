#!/usr/bin/python3
# -*-coding:utf-8 -*-
"""
@File    :   __init__.py
@Time    :   2022/11/30
@Author  :   Yuqian Shi
@Version :   0.1
"""

import pyarrow.flight as flight
import pyarrow as pa
import pyarrow.parquet as pq
import base64
import secrets
import argparse
import pathlib
import pyarrow.dataset as ds
from COCA_Base import *
import threading
import time
import pytz
import copy


def get_broker(conf, log_level):
    """
    return the broker instance,will start serving once instance created?
    @param conf: config file path or dict
    @param log_level: log level
    """
    if type(conf) is str:
        conf = load_dict(conf)
    broker = BaseBroker(copy.deepcopy(conf),
                        auth_handler=NoOpAuthHandler(),
                        middleware={
        "basic": BasicAuthBrokerMiddlewareFactory(conf["accounts"]["all_users"])
    }, log_level=log_level
    )
    return broker


class BaseBroker(flight.FlightServerBase):
    def __init__(self, conf, log_level, ** kwargs):
        """
        base broker class of COCA Broker
        behavior similar to a MQTT broker, but only support QoS 0
        @param conf: config file path or dict
        @param log_level: log level
        @param kwargs: other parameters for flight server
        """
        # host = "localhost", location = None,
        # tls_certificates = None, verify_client = False,
        # root_certificates = None, auth_handler = None

        # first process and valid config file.
        # each flight is a collection or a channel with root apis
        # e.g. root_api:["root1","root2"] (corresponding to user groups)
        self.data = {}
        self.logger = get_logger(log_level)
        self._init_config(conf)
        super(BaseBroker, self).__init__(location=self.location, ** kwargs)
        self.clients_dict = {}  # store connected clients,including ids,writer and reader
        self.topic_dict = {}  # store who subscribed what
        self.logger.debug(list(dir(self)))
        # self.root_apis = root_apis
        # here data is used as a map to all resources

    def descriptor_to_key(self, descriptor):
        """
        Converts a descriptor to a key.
        @param descriptor: a flight descriptor
        """
        return (descriptor.descriptor_type.value, descriptor.command,
                tuple(descriptor.path or tuple()))

    def _build_access_table(self):
        """
        build access table according to config file
        """
        self.access_table = {}
        t = self.conf["resource_map"]["resources"]
        for api_name, v in t.items():
            for resource_name, sub_v in v["resources"].items():
                topic = api_name+"/"+resource_name
                temp = sub_v["resource_meta"]["access"]
                for _, inds in temp.items():
                    if max(inds) >= len(self.conf["resource_map"]["granted_type"]):
                        raise ValueError(" invalid access index")
                self.access_table[topic] = sub_v["resource_meta"]["access"]
                self.data[topic] = {}
        self.logger.debug("access table built")

    def _validate_conf(self, conf):
        """
        check if the config file is valid
        """
        result, msg = if_list_have_duplicates(conf)
        if result:
            raise ValueError(msg)
        if_required_keys_exists(
            ["accounts", "resource_map", "broker_info"], conf)
        if_required_keys_exists(
            ["broker_meta", "host", "port", "scheme"], conf["broker_info"])
        if_required_keys_exists(["admin_users"], conf["accounts"])
        if len(conf["accounts"]["admin_users"]) == 0:
            raise ValueError("no admin provided")
        self.conf = conf
        self.logger.debug("conf check passed")

    def _init_meta(self, conf):
        """
        Initializes the meta attribute with broker, API, and resource metadata.
        @param conf: config file path or dict
        """
        self.meta = {
            "broker_meta": self.broker_info["broker_meta"],
            "api_meta": {},
            "resource_meta": {}
        }
        t = conf["resource_map"]["resources"]
        for api_key, api_obj in t.items():
            self.meta["api_meta"][api_key] = api_obj["api_meta"]
            for topic, resource_obj in api_obj["resources"].items():
                self.meta["resource_meta"][topic] = resource_obj["resource_meta"]
        del self.broker_info["broker_meta"]

    def _init_config(self, conf):
        """
        build fundamental objects: self.access_table, self.data and self.meta
        self.data contains the data to all resources via the topic,
        which is apiname/resourcename(a collection or channel),
        the metadata  stores all meta info of all apis and resources
        """
        self._validate_conf(conf)

        self.broker_info = conf["broker_info"]
        self.location = "{0}://{1}:{2}".format(
            self.broker_info["scheme"], self.broker_info["host"], self.broker_info["port"])
        self._build_access_table()
        self._init_meta(conf)
        self.user_groups = conf.get("accounts")

    def _make_flight_info(self, dataset):
        """
        Creates a FlightInfo object for a given dataset.
        @param dataset: The dataset to create a FlightInfo object for.
        @return: The FlightInfo object.
        """
        dataset_path = self._repo/dataset_path
        schema = pq.read_schema(dataset_path)
        meta = flight.read_metadata(dataset_path)
        descriptor = flight.FlightDescriptor.for_path(dataset.encode("utf-8"))
        endpoints = [pyarrow.flight.FlightEndpoint(dataset, [self.location])]
        return flight.FlightInfo(schema, descriptor, endpoints, meta.num_rows, meta.serilized_size)

    def _make_flight_info_dataset(self, criteria):
        """
        Creates a FlightInfo object for a given dataset.
        @criteria: The criteria to match the dataset.
        @return: The FlightInfo object.
        """
        ds_name = criteria.decode("utf-8")

        dataset = self._make_dataset(criteria)

        schema = dataset.schema
        descriptor = flight.FlightDescriptor.for_path(criteria)
        endpoints = [flight.FlightEndpoint(ds_name, [self._location])]

        return flight.FlightInfo(schema,
                                 descriptor,
                                 endpoints,
                                 dataset.count_rows(),
                                 -1)

    def _make_dataset(self, criteria):
        """
        save data to disk
        @param criteria: The criteria to match the dataset.
        @return: The generated dataset.
        """
        ds_name = criteria.decode("utf-8")
        name, name_suffix = ds_name.split(".")

        ds_paths = list(self._repo.glob("**/"+name+"_*.parquet"))
        ds_paths.sort()

        dataset = ds.dataset(ds_paths, format="parquet")

        return dataset

    def _open_channel(self, channel_key):
        """
        Opens a channel for a given key.Raises an exception if the channel does not exist.
        @param channel_key: The key of the channel to open.
        
        """
        try:
            stream = pa.input_stream(source)
        except Exception as e:
            print(e)

    def _discovery(self):
        """
        Discovers the broker metadata.
        @return: The broker metadata.
        """
        t = self.meta["broker_meta"]
        # change data format for transmission
        t = {"title": t["title"],
             "description": t["description"],
             "contact": t["contact"],
             "api_roots": t["default"]}
        return t

    def _get_user_groups(self, user):
        """
        @param user: user name
        @return: user groups of the user
        """
        temp = self.user_groups
        result = []
        for k, v in temp.items():
            if user in v:
                result.append(k)
        result.remove("all_users")
        return result

    def _get_user_name_from_context(self, context):
        """
        @param context: context
        @return: user name of the user
        """
        middleware = context.get_middleware("basic")
        return middleware.username

    def _get_req_time_from_context(self, context):
        """
        @param context: context
        @return: request time of the request
        """
        middleware = context.get_middleware("basic")
        return middleware.req_time

    def _verify_user_access(self, context, requested_resource, requested_access_types):
        """
        return None if user have all the requested access, otherwise, raise an exception
        @param context: context
        @param requested_resource: requested resource
        @param requested_access_types: requested access types
        @return: None
        """
        # self.logger.debug(
        #     "----------------begin of _verify_user_access----------------")
        username = self._get_user_name_from_context(context)
        # only take front two for now
        topic = requested_resource
        # TODO: replace with better raise
        # check if requested resource exists
        if topic not in self.data:
            raise flight.FlightServerError(
                "topic not found,please check your topic_name: %s" % topic)
        # check if user has enough permission
        # self.logger.debug(username in self.user_groups["admin_users"])
        if username in self.user_groups["admin_users"]:
            return username
        # admin users can do everything
        temp = self.access_table[topic]
        raw_username = username
        if not username in temp:
            # check if user in user groups
            user_groups = self._get_user_groups(username)
            username = user_groups
            find_count = 0
            for group in user_groups:
                if group in temp:
                    find_count += 1
            if find_count == 0:
                raise flight.FlightUnauthenticatedError(
                    "{0} do not have any access to resource {1}".format(username, requested_resource))
        granted_type = self.conf["resource_map"]["granted_type"]
        if type(username) is not list:
            # check using username
            temp = self.access_table[topic][username]
            for access in requested_access_types:
                index = granted_type.index(access)
                if not index in temp:
                    raise flight.FlightUnauthenticatedError(
                        "{0} do not have {1} to resource {2}".format(username, requested_access_types, requested_resource))
        else:
            # check using user group
            granted_accesses = []
            for group in username:
                temp = self.access_table[topic][group]
                for access in requested_access_types:
                    index = granted_type.index(access)
                    if index in temp:
                        if not access in granted_accesses:
                            granted_accesses.append(access)
            diff = list_differences(
                requested_access_types, granted_accesses)
            if diff == []:
                return raw_username
            raise flight.FlightUnauthorizedError(
                "{0} do not have {1} to resource {2}".format(middleware.username, diff, requested_resource))

    def list_flights(self, context, criteria):
        '''
        @param context: context
        @param criteria: criteria
        @return: a list of flight info
        '''
        # self.logger.debug(
        #     "----------------begin of  list_flights----------------")
        if criteria == b"":
            for dataset in self._repo.glob("**/*.parquet"):
                yield self._make_flight_info(dataset.name)

        elif criteria.decode("utf-8").endswith(".dataset"):
            yield self._make_flight_info_dataset(criteria)
        else:
            for dataset in self._repo.glob(criteria.decode("utf-8")):
                yield self._make_flight_info(dataset.name)
        # self.logger.debug(
        #     "----------------end of  list_flights----------------")

    def get_root_api_info(self, context, descriptor):
        '''
        @param context: context
        @param descriptor: descriptor
        @return: a flight info
        '''
        if descriptor.path[0].decode("utf-8").endswith(".dataset"):
            fi = self._make_flight_info_dataset(descriptor.path[0])
        else:
            fi = self._make_flight_info(descriptor.path[0].decode("utf-8"))

        return fi

    def _verify_exchange_access(self, context, descriptor):
        '''
        return read_access, write_access, topic, user_name if no exception raised
        @param context: context
        @param descriptor: descriptor
        '''
        read_access = False
        write_access = False
        topic = descriptor.path[0].decode("utf-8")
        user_name = self._get_user_name_from_context(context)
        try:
            self._verify_user_access(
                context, topic, ["write"])
            write_access = True
        except Exception as e:
            self.logger.debug(e)
            pass
        try:
            self._verify_user_access(
                context, topic, ["read"])
            read_access = True
        except Exception as e:
            self.logger.debug(e)
            pass
        if read_access == write_access == False:
            raise flight.FlightUnauthenticatedError("{} do not have access to channel {}".format(
                user_name, topic))
        return read_access, write_access, topic, user_name

    def _check_retention(self, retention):
        raise NotImplementedError

    def _send_to_multiple_writer(self, writers, data):
        '''
        @param writers: a list of writers
        @param data: data to be sent
        raise exception if any error occurs
        '''
        self.logger.debug(
            "----------------begin of _send_to_multiple_writer----------------")
        self.logger.debug(len(writers))
        for w in writers:
            try:
                w.begin(data.schema)
            except Exception as e:
                self.logger.debug(e)
            try:
                w.write(data)
            except Exception as e:
                self.logger.debug(e)
                # This writer has already been started.

    def do_exchange(self, context, descriptor, reader, writer):
        '''
        @param context: context
        @param descriptor: descriptor
        @param reader: reader
        @param writer: writer
        @return: None
        '''
        self.logger.debug(
            "----------------begin of do_exchange----------------")
        key = self.descriptor_to_key(descriptor)
        topic = descriptor.path[0].decode("utf-8")
        # do_put needs write access,check if the user is accessing permitted resources
        r_access, w_access, topic, user_name = self._verify_exchange_access(
            context, descriptor)

        req_time = str_to_dt(self._get_req_time_from_context(context))
        t = self.clients_dict
        if not topic in t:
            t[topic] = {}
        if not user_name in t[topic]:
            t[topic][user_name] = {}
        if r_access:
            t[topic][user_name]["writer"] = writer
        if w_access:
            t[topic][user_name]["reader"] = reader
        for chunk in reader:
            data = chunk.data

            list_of_writers = []
            for user in t[topic]:
                if user != user_name:
                    if "writer" in t[topic][user]:
                        list_of_writers.append(t[topic][user]["writer"])
            self._send_to_multiple_writer(list_of_writers, chunk.data)
        self.logger.debug(
            "----------------end of do_exchange----------------")

    def do_action(self, context, action):
        """
        do actions
        """
        self.logger.debug(
            "----------------begin of do_action----------------")
        user = self._get_user_name_from_context(context)

        action_type = (action.type.encode("utf-8"))
        if action_type == b"shutdown":
            if "admin_users" in self. _get_user_groups(user):
                # only admins can shut down broker
                a = threading.Thread(target=self._shutdown)
                a.start()
                return [action_type, b"broker closing"]
            else:
                raise flight.FlightUnauthorizedError(
                    "{0} do not have access to shutdown broker".format(user))
        elif action_type == b"discovery":
            return [action_type, bytes(json.dumps(self._discovery()), "utf-8")]
        elif action_type == b"ping":
            return [b"pong"]
        return [action_type, action.body.to_pybytes()]
        # Read the uploaded data and write to Parquet incrementally

    def _update_manifest(self, new_obj, topic, request_time):
        '''
        Updates the manifest with a new object.
        @param new_obj: new object
        @param topic: topic
        @param request_time: request time
        '''
        media_type_fmt = "application/stix+json;version={0}"
        media_type = media_type_fmt.format(
            determine_spec_version(new_obj))
        # manifest locates in meta
        _id = new_obj.get("id")
        v = determine_version(new_obj, request_time)
        topic = topic.split("/")[-1]
        if not _id in self.meta["resource_meta"][topic]["manifest"]:
            self.meta["resource_meta"][topic]["manifest"][_id] = {}
        self.meta["resource_meta"][topic]["manifest"][_id][v] = {
            "id": _id,
            "date_added": dt_to_str(request_time),
            "version": v,
            "media_type": media_type,
        }

        # if the media type is new, attach it to the collection
        if media_type not in self.meta["resource_meta"][topic]["media_types"]:
            self.meta["resource_meta"][topic]["media_types"].append(
                media_type)

        #     # quit once you have found the collection that needed updating
        #     break
    def _add_to_resource(self, data, topic, req_time):
        """
        add a list of object to the requested path,will update manifest and return status
        @param data: a list of objects
        @param topic: topic
        @param req_time: request time
        @return: failed, succeeded, pending, successes, failures
        """
        if topic not in self.data:
            self.data[topic] = {}
        failed = 0
        succeeded = 0
        pending = 0
        successes = []
        failures = []
        while len(data) > 0:
            i = data.pop()
            msg = None
            i_version = determine_version(i, req_time)
            try:
                i["_date_added"] = i_version
                if not i["id"] in self.data[topic]:
                    self.data[topic][i["id"]] = {}
                if not i_version in self.data[topic][i["id"]]:
                    self.data[topic][i["id"]][i_version] = i
                    msg = "object added"
                    self._update_manifest(
                        i, topic, req_time)

                status_details = generate_status_details(
                    i["id"], i_version, msg)
                successes.append(status_details)
                succeeded += 1
            except Exception as e:
                failed += 1
                status_details = generate_status_details(i["id"], i_version, e)
                failures.append(status_details)
                self.logger.debug(e)
                raise ProcessingError(
                    "While processing supplied content, an error occurred", 422, e)
        return failed, succeeded, pending, successes, failures

    def do_put(self, context, descriptor, reader, writer, ** kwargs):
        self.logger.debug(
            "----------------begin of do_puts----------------")
        key = self.descriptor_to_key(descriptor)
        topic = descriptor.path[0].decode("utf-8")
        # do_put needs write access,check if the user is accessing permitted resources
        self._verify_user_access(context, topic, ["write"])
        req_time = str_to_dt(self._get_req_time_from_context(context))
        for chunk in reader:
            data = chunk.data.to_pydict()
            data = data["objects"]
            failed, succeeded, pending, successes, failures = self._add_to_resource(
                data, topic, req_time)
            status = generate_status(
                dt_to_str(req_time), "complete", succeeded,
                failed, pending, successes=successes,
                failures=failures,
            )
            self.meta["api_meta"][topic.split(
                "/")[0]]["status"].append(status)

    def do_get(self, context, ticket):
        # key = self.descriptor_to_key(descriptor)
        # dataset = descriptor.path[0].decode("utf-8")
        self._verify_user_access(context, dataset, ["read"])

        key = ast.literal_eval(ticket.ticket.decode())
        if key not in self.flights:
            return None
        return pyarrow.flight.RecordBatchStream(self.flights[key])

    def _shutdown(self, delay=0):
        """Shut down after a delay."""
        time.sleep(delay)
        self.shutdown()

    def list_actions(self, context):
        return [
            ("shutdown", "Shut down this broker."),
            # ("get", "get data from broker"),
            # ("put", "put data to broker"),
            # ("discovery", "discovery broker info"),
            # ("ping", "for RTT testing")
        ]


class BasicAuthBrokerMiddlewareFactory(pa.flight.ServerMiddlewareFactory):
    """
    Middleware that implements username-password authentication.
    check if the user has enough permissions to perform the requested action.
    Parameters
    ----------
    creds: Dict[str, str]
        A dictionary of username-password values to accept.
    """

    def __init__(self, creds):
        """
        TBA
        """
        # user name and password
        self.creds = creds

        # Map generated bearer tokens to users
        self.tokens = {}

    def start_call(self, info, headers):
        """Validate credentials at the start of every call."""
        # Search for the authentication header (case-insensitive)
        auth_header = None
        # TODO: set this value in client
        request_time = dt_to_str(get_timestamp())
        for header in headers:
            if header.lower() == "authorization":
                auth_header = headers[header][0]
                break

        if not auth_header:
            raise pa.flight.FlightUnauthenticatedError(
                "No credentials supplied")

        # The header has the structure "AuthType TokenValue", e.g.
        # "Basic <encoded username+password>" or "Bearer <random token>".
        auth_type, _, value = auth_header.partition(" ")

        if auth_type == "Basic":
            # Initial "login". The user provided a username/password
            # combination encoded in the same way as HTTP Basic Auth.
            decoded = base64.b64decode(value).decode("utf-8")
            username, _, password = decoded.partition(":")
            if not password or password != self.creds.get(username):
                raise pa.flight.FlightUnauthenticatedError(
                    "Unknown user or invalid password")
            # Generate a secret, random bearer token for future calls.
            token = secrets.token_urlsafe(32)
            self.tokens[token] = username
            return BasicAuthBrokerMiddleware(token, username, request_time)
        elif auth_type == "Bearer":
            # An actual call. Validate the bearer token.
            username = self.tokens.get(value)
            if username is None:
                raise pa.flight.FlightUnauthenticatedError("Invalid token")
            self.username = username
            return BasicAuthBrokerMiddleware(value, username, request_time)
        raise pa.flight.FlightUnauthenticatedError("No credentials supplied")


class BasicAuthBrokerMiddleware(pa.flight.ServerMiddleware):
    """
    Middleware that implements username-password authentication.
    """

    def __init__(self, token, username, req_time):
        self.token = token
        self.username = username
        self.req_time = req_time

    def sending_headers(self):
        """Return the authentication token to the client."""
        return {"authorization": f"Bearer {self.token}"}


class NoOpAuthHandler(pa.flight.ServerAuthHandler):
    """
    A handler that implements username-password authentication.
    This is required only so that the broker will respond to the internal
    Handshake RPC call, which the client calls when authenticate_basic_token
    is called. Otherwise, it should be a no-op as the actual authentication is
    implemented in middleware.
    """

    def authenticate(self, outgoing, incoming):
        pass

    def is_valid(self, token):
        return ""
