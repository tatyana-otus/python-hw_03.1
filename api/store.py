#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import redis
import time
import functools


class Store:
    def __init__(self, redis_config,
                 reconnect_attempts=100,
                 reconnect_delay=0.01):
        self.reconnect_attempts = reconnect_attempts
        self.reconnect_delay = reconnect_delay
        self.config = redis_config
        self.db = None
        self.connect_to_db = None
        self.read_from_db = None
        self.write_from_db = None

    def connect(self):
        self.db = redis.StrictRedis(**self.config, decode_responses=True,
                                    socket_timeout=1,
                                    socket_connect_timeout=1)
        self.connect_to_db = self._reconnect(self.db.ping)
        self.read_from_db = self._reconnect(self.db.get)
        self.write_from_db = self._reconnect(self.db.set)
        self.connect_to_db()

    def cache_get(self, key):
        data = None
        try:
            if self.db is None:
                self.connect()
            data = self.read_from_db(key)
        except ConnectionError as e:
            logging.error("Connection error: {}".format(e))
        return data

    def cache_set(self, key, value, ttl_sec):
        try:
            if self.db is None:
                self.connect()
            self.write_from_db(key, value, nx=ttl_sec)
        except ConnectionError as e:
            logging.error("Connection error: {}".format(e))

    def _reconnect(self, func, *args, **kwargs):
        def wrapper(*args, **kwargs):
            attempts = self.reconnect_attempts
            while True:
                try:
                    return func(*args, **kwargs)
                except redis.exceptions.ConnectionError as e:
                    time.sleep(self.reconnect_delay)
                    logging.error("Connection error: reconnecting ... ")
                    attempts -= 1
                    if attempts == 0:
                        raise ConnectionError("Can't connect to DB")
            return result
        return wrapper

    def get(self, key):
        if self.db is None:
            self.connect()
        data = self.read_from_db(key)
        if data is None:
            raise ValueError("{} key doesn’t exist".format(key))
        return data
