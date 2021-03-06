import unittest
import redis
import subprocess

from api import store
from tests import helper


TEST_PORT = 9006
redis_up_config = {
    "host": "localhost",
    "port": TEST_PORT,
    "db": 0
}


class TestSuiteStoreOk(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.redis_process = subprocess.Popen(['redis-server', '--port',
                                             str(redis_up_config["port"])],
                                             stdout=subprocess.DEVNULL)
        connect = False
        r = redis.StrictRedis(**redis_up_config)
        while connect is False:
            try:
                connect = r.ping()
            except redis.exceptions.ConnectionError as e:
                pass

    @classmethod
    def tearDownClass(cls):
        cls.redis_process.terminate()
        cls.redis_process.wait()

    def setUp(self):
        self.store = store.Store(redis_up_config, reconnect_attempts=5)
        self.store.connect()

    def tearDown(self):
        r = redis.StrictRedis(**redis_up_config)
        r.flushdb()

    @helper.cases([("key_0", "111"), ("key_1", 4), ("key_2", 2.3), ("key_3", "2.3")])
    def test_store_cache_set_get_float(self, key, value):
        self.store.cache_set(key, value, 1)
        result = self.store.cache_get(key)
        self.assertEqual(result, str(value))

    @helper.cases([("key_0", "qwqw"), ("key_1", "121awa")])
    def test_store_cache_set_get(self, key, value):
        self.store.cache_set(key, value, 1)
        result = self.store.cache_get(key)
        self.assertEqual(result, value)

    @helper.cases([("key_0", "111"), ("key_1", 4), ("key_2", 2.3), ("key_3", "2.3")])
    def test_store_get_float(self, key, value):
        self.store.db.set(key, value, 1)
        result = self.store.get(key)
        self.assertEqual(result, str(value))

    @helper.cases([("key_0", "qwqw"), ("key_1", "121awa")])
    def test_store_get(self, key, value):
        self.store.db.set(key, value, 1)
        result = self.store.get(key)
        self.assertEqual(result, value)

    def test_empty_store_get(self,):
        with self.assertRaises(ValueError):
            self.store.get("key_0")

    def test_empty_store_cache_get(self,):
        result = self.store.cache_get("key_0")
        self.assertEqual(result, None)


class TestSuiteStoreDown(unittest.TestCase):

    def setUp(self):
        self.store = store.Store(redis_up_config, reconnect_attempts=5, connect_now=False)

    def test_store_connect(self):
        with self.assertRaises(ConnectionError):
            self.store.connect()

    @helper.cases([("key_0", "111"), ("key_1", 4), ("key_2", "asasa")])
    def test_store_cache_set_without_connect(self, key, value):
        with self.assertRaises(TypeError):
            self.store.cache_set(key, value, 1)

    @helper.cases(["key_0", "key_1", "key_2"])
    def test_store_cache_get_without_connect(self, key):
        with self.assertRaises(TypeError):
            self.store.cache_get(key)

    def test_store_get_without_connect(self):
        with self.assertRaises(TypeError):
            self.store.get("key_0")

    @helper.cases([("key_0", "111"), ("key_1", 4), ("key_2", "asasa")])
    def test_store_cache_set_get_with_connect(self, key, value):
        try:
            self.store.connect()
        except ConnectionError:
            pass
        self.store.cache_set(key, value, 1)
        result = self.store.cache_get(key)
        self.assertEqual(result, None)

    def test_store_get_with_connect(self):
        try:
            self.store.connect()
        except ConnectionError:
            pass
        with self.assertRaises(ConnectionError):
            self.store.get("key_0")

if __name__ == "__main__":
    unittest.main()
