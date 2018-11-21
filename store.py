#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import redis


class Store:
    def __init__(self, **kwargs):
        self.db = redis.StrictRedis(**kwargs)
        try:
            self.db.ping()
        except redis.exceptions.ConnectionError as e:
            logging.error("Connection error: {}".format(e))

    def cache_get(self, key):
        b_str = None
        try:
            b_str = self.db.get(key)
        except redis.exceptions.ConnectionError as e:
            logging.error("Connection error: {}".format(e))

        return float(b_str.decode("utf-8")) if b_str is not None else 0

    def cache_set(self, key, value, ttl_sec):
        try:
            self.db.set(key, value, nx=ttl_sec)
        except redis.exceptions.ConnectionError as e:
            logging.error("Connection error: {}".format(e))

    def get(self, key):
        attempts = 10
        b_str = None
        while attempts:
            try:
                b_str = self.db.get(key)
                attempts = 0
            except redis.exceptions.ConnectionError as e:
                logging.error("Connection error: reconnecting ... ")
                attempts -= 1
                if attempts == 0:
                    raise ConnectionError("Can't connect to DB")
        if b_str is None:
            raise ValueError("{} key doesn’t exist".format(key))

        return b_str.decode("utf-8")
