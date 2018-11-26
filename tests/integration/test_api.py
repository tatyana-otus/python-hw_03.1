import hashlib
import datetime
import functools
import unittest
import redis
import subprocess
import json

from api import api
from api import store

TEST_PORT = 9006
redis_config = {
    "host": "localhost",
    "port": TEST_PORT,
    "db": 0
}


def cases(cases):
    def decorator(f):
        @functools.wraps(f)
        def wrapper(*args):
            for c in cases:
                new_args = args + (c if isinstance(c, tuple) else (c,))
                try:
                    f(*new_args)
                except AssertionError as e:
                    raise AssertionError("{}: {} (test case: {})".format(e, f.__name__, c))
        return wrapper
    return decorator


class TestSuiteApiWithStore(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.redis_process = subprocess.Popen(['redis-server', '--port',
                                             str(redis_config["port"])],
                                             stdout=subprocess.DEVNULL)
        connect = False
        r = redis.StrictRedis(**redis_config)
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
        self.context = {}
        self.headers = {}
        self.store = store.Store(redis_config, reconnect_attempts=5)
        self.store.connect()

    def tearDown(self):
        r = redis.StrictRedis(**redis_config)
        r.flushdb()

    def get_response(self, request):
        return api.method_handler({"body": request, "headers": self.headers},
                                  self.context, self.store)

    def set_valid_auth(self, request):
        if request.get("login") == api.ADMIN_LOGIN:
            d = datetime.datetime.now().strftime("%Y%m%d%H") + api.ADMIN_SALT
            request["token"] = hashlib.sha512(d.encode('utf-8')).hexdigest()
        else:
            msg = request.get("account", "") + request.get("login", "") + api.SALT
            request["token"] = hashlib.sha512(msg.encode('utf-8')).hexdigest()

    def get_score_key(self, arguments):
        first_name = arguments.get("first_name")
        last_name = arguments.get("last_name")
        phone = str(arguments.get("phone", ""))
        birthday = arguments.get("birthday")
        if birthday:
            birthday = datetime.datetime.strptime(birthday, "%d.%m.%Y")
        key_parts = [
            first_name or "",
            last_name or "",
            phone or "",
            birthday.strftime("%Y%m%d") if birthday is not None else "",
        ]
        return "uid:" + hashlib.md5("".join(key_parts).encode('utf-8')).hexdigest()

    def calculate_score(self, arguments):
        score = 0
        if arguments.get("phone"):
            score += 1.5
        if arguments.get("email"):
            score += 1.5
        if arguments.get("birthday") and arguments.get("gender"):
            score += 1.5
        if arguments.get("first_name") and arguments.get("last_name"):
            score += 0.5
        return score

    def save_score_key_value(self, key, value):
        r = redis.StrictRedis(**redis_config)
        r.set(key, value)

    def fill_db_interests(self):
        r = redis.StrictRedis(**redis_config)
        r.set("i:0", '["poker", "alcohol"]')
        r.set("i:1", '["yoga", "theater"]')
        r.set("i:2", '["pole dance", "zumba"]')
        r.set("i:3", '["football", "beer"]')

    def fill_db_interests_wrong_format(self):
        r = redis.StrictRedis(**redis_config)
        r.set("i:0", 'poker", "alcohol"')

    def test_empty_request(self):
        _, code = self.get_response({})
        self.assertEqual(api.INVALID_REQUEST, code)

    def test_ok_score_admin_request(self):
        arguments = {"phone": "79175002040", "email": "stupnikov@otus.ru"}
        request = {"account": "horns&hoofs", "login": "admin",
                   "method": "online_score", "arguments": arguments}
        self.set_valid_auth(request)
        response, code = self.get_response(request)
        self.assertEqual(api.OK, code)
        score = response.get("score")
        self.assertEqual(score, 42)

    @cases([
        {"phone": "79175002040", "email": "stupnikov@otus.ru"},
        {"phone": 79175002040, "email": "stupnikov@otus.ru"},
        {"gender": 1, "birthday": "01.01.2000", "first_name": "a",
         "last_name": "b"},
        {"gender": 0, "birthday": "01.01.2000"},
        {"gender": 2, "birthday": "01.01.2000"},
        {"first_name": "a", "last_name": "b"},
        {"phone": "79175002040", "email": "stupnikov@otus.ru", "gender": 1,
         "birthday": "01.01.2000", "first_name": "a", "last_name": "b"},
    ])
    def test_ok_score_request_fail_cache_get(self, arguments):
        request = {"account": "horns&hoofs", "login": "h&f",
                   "method": "online_score", "arguments": arguments}
        self.set_valid_auth(request)
        response, code = self.get_response(request)
        self.assertEqual(api.OK, code)
        score = response.get("score")
        self.assertTrue(isinstance(score, (int, float)) and score >= 0)
        self.assertEqual(score, self.calculate_score(arguments))
        self.assertEqual(sorted(self.context["has"]), sorted(arguments.keys()))

    @cases([
        {"phone": "79175002040", "email": "stupnikov@otus.ru"},
        {"phone": 79175002040, "email": "stupnikov@otus.ru"},
        {"gender": 1, "birthday": "01.01.2000", "first_name": "a",
         "last_name": "b"},
        {"gender": 0, "birthday": "01.01.2000"},
        {"gender": 2, "birthday": "01.01.2000"},
        {"first_name": "a", "last_name": "b"},
        {"phone": "79175002040", "email": "stupnikov@otus.ru",
         "gender": 1, "birthday": "01.01.2000", "first_name": "a",
         "last_name": "b"},
    ])
    def test_ok_score_request_ok_cache_get(self, arguments):
        request = {"account": "horns&hoofs", "login": "h&f",
                   "method": "online_score", "arguments": arguments}
        key = self.get_score_key(arguments)
        self.save_score_key_value(key, 123.987)
        self.set_valid_auth(request)
        response, code = self.get_response(request)
        self.assertEqual(api.OK, code)
        score = response.get("score")
        self.assertEqual(score, 123.987)
        self.assertEqual(sorted(self.context["has"]), sorted(arguments.keys()))

    @cases([
        {"phone": "79175002040", "email": "stupnikov@otus.ru"}
    ])
    def test_ok_score_request_wrong_db_format(self, arguments):
        request = {"account": "horns&hoofs", "login": "h&f",
                   "method": "online_score", "arguments": arguments}
        key = self.get_score_key(arguments)
        self.save_score_key_value(key, "one")
        self.set_valid_auth(request)
        response, code = self.get_response(request)
        self.assertEqual(api.OK, code)
        score = response.get("score")
        self.assertEqual(score, "one",)

    @cases([
        {"client_ids": [1, 2, 3], "date": datetime.datetime.today().strftime("%d.%m.%Y")},
        {"client_ids": [1, 2], "date": "19.07.2017"},
        {"client_ids": [0]},
    ])
    def test_ok_interests_request_ok_get(self, arguments):
        self.fill_db_interests()
        request = {"account": "horns&hoofs", "login": "h&f",
                   "method": "clients_interests", "arguments": arguments}
        self.set_valid_auth(request)
        response, code = self.get_response(request)
        self.assertEqual(api.OK, code)
        self.assertEqual(len(arguments["client_ids"]), len(response))
        self.assertTrue(all(v and isinstance(v, list) and all(isinstance(i, str) for i in v)
                        for v in response.values()))
        self.assertEqual(self.context.get("nclients"), len(arguments["client_ids"]))

    @cases([
         {"client_ids": [0]},
    ])
    def test_ok_interests_wrong_db_format(self, arguments):
        self.fill_db_interests_wrong_format()
        request = {"account": "horns&hoofs", "login": "h&f",
                   "method": "clients_interests", "arguments": arguments}
        self.set_valid_auth(request)
        with self.assertRaises(json.decoder.JSONDecodeError):
            self.get_response(request)


class TestSuiteApiWithStoreDown(unittest.TestCase):

    def setUp(self):
        self.context = {}
        self.headers = {}
        self.store = store.Store(redis_config, reconnect_attempts=5)

    def get_response(self, request):
        return api.method_handler({"body": request, "headers": self.headers},
                                  self.context, self.store)

    def set_valid_auth(self, request):
        if request.get("login") == api.ADMIN_LOGIN:
            d = datetime.datetime.now().strftime("%Y%m%d%H") + api.ADMIN_SALT
            request["token"] = hashlib.sha512(d.encode('utf-8')).hexdigest()
        else:
            msg = request.get("account", "") + request.get("login", "") + api.SALT
            request["token"] = hashlib.sha512(msg.encode('utf-8')).hexdigest()

    def test_empty_request(self):
        _, code = self.get_response({})
        self.assertEqual(api.INVALID_REQUEST, code)

    def test_ok_score_admin_request(self):
        arguments = {"phone": "79175002040", "email": "stupnikov@otus.ru"}
        request = {"account": "horns&hoofs", "login": "admin",
                   "method": "online_score", "arguments": arguments}
        self.set_valid_auth(request)
        response, code = self.get_response(request)
        self.assertEqual(api.OK, code)
        score = response.get("score")
        self.assertEqual(score, 42)

    @cases([
        {"client_ids": [1, 2, 3], "date": datetime.datetime.today().strftime("%d.%m.%Y")},
        {"client_ids": [1, 2], "date": "19.07.2017"},
        {"client_ids": [0]},
    ])
    def test_ok_interests_request(self, arguments):
        request = {"account": "horns&hoofs", "login": "h&f",
                   "method": "clients_interests", "arguments": arguments}
        self.set_valid_auth(request)
        with self.assertRaises(ConnectionError):
            self.get_response(request)

    @cases([
        {"phone": "79175002040", "email": "stupnikov@otus.ru"},
        {"phone": 79175002040, "email": "stupnikov@otus.ru"},
        {"gender": 1, "birthday": "01.01.2000", "first_name": "a",
         "last_name": "b"},
        {"gender": 0, "birthday": "01.01.2000"},
        {"gender": 2, "birthday": "01.01.2000"},
        {"first_name": "a", "last_name": "b"},
        {"phone": "79175002040", "email": "stupnikov@otus.ru",
         "gender": 1, "birthday": "01.01.2000",
         "first_name": "a", "last_name": "b"},
    ])
    def test_ok_score_request(self, arguments):
        request = {"account": "horns&hoofs", "login": "h&f",
                   "method": "online_score", "arguments": arguments}
        self.set_valid_auth(request)
        response, code = self.get_response(request)
        self.assertEqual(api.OK, code)
        score = response.get("score")
        self.assertTrue(isinstance(score, (int, float)) and score >= 0)
        self.assertEqual(sorted(self.context["has"]), sorted(arguments.keys()))


class TestSuiteApiInvalidRequests(unittest.TestCase):

    def setUp(self):
        self.context = {}
        self.headers = {}
        self.store = store.Store(redis_config, reconnect_attempts=5)

    def get_response(self, request):
        return api.method_handler({"body": request, "headers": self.headers},
                                  self.context, self.store)

    def set_valid_auth(self, request):
        if request.get("login") == api.ADMIN_LOGIN:
            d = datetime.datetime.now().strftime("%Y%m%d%H") + api.ADMIN_SALT
            request["token"] = hashlib.sha512(d.encode('utf-8')).hexdigest()
        else:
            msg = request.get("account", "") + request.get("login", "") + api.SALT
            request["token"] = hashlib.sha512(msg.encode('utf-8')).hexdigest()

    @cases([
        {"account": "horns&hoofs", "login": "h&f", "method": "online_score"},
        {"account": "horns&hoofs", "login": "h&f", "arguments": {}},
        {"account": "horns&hoofs", "method": "online_score", "arguments": {}},
    ])
    def test_invalid_method_request(self, request):
        self.set_valid_auth(request)
        response, code = self.get_response(request)
        self.assertEqual(api.INVALID_REQUEST, code)
        self.assertTrue(len(response), request)

    @cases([
        {},
        {"phone": "79175002040"},
        {"phone": "89175002040", "email": "stupnikov@otus.ru"},
        {"phone": "79175002040", "email": "stupnikovotus.ru"},
        {"phone": "79175002040", "email": "stupnikov@otus.ru",
         "gender": -1},
        {"phone": "79175002040", "email": "stupnikov@otus.ru",
         "gender": "1"},
        {"phone": "79175002040", "email": "stupnikov@otus.ru",
         "gender": 1, "birthday": "01.01.1890"},
        {"phone": "79175002040", "email": "stupnikov@otus.ru",
         "gender": 1, "birthday": "XXX"},
        {"phone": "79175002040", "email": "stupnikov@otus.ru",
         "gender": 1, "birthday": "01.01.2000", "first_name": 1},
        {"phone": "79175002040", "email": "stupnikov@otus.ru",
         "gender": 1, "birthday": "01.01.2000",
         "first_name": "s", "last_name": 2},
        {"phone": "79175002040", "birthday": "01.01.2000",
         "first_name": "s"},
        {"email": "stupnikov@otus.ru", "gender": 1, "last_name": 2},
    ])
    def test_invalid_score_request(self, arguments):
        request = {"account": "horns&hoofs", "login": "h&f",
                   "method": "online_score", "arguments": arguments}
        self.set_valid_auth(request)
        response, code = self.get_response(request)
        self.assertEqual(api.INVALID_REQUEST, code)
        self.assertTrue(len(response), arguments)

    @cases([
        {},
        {"date": "20.07.2017"},
        {"client_ids": [], "date": "20.07.2017"},
        {"client_ids": {1: 2}, "date": "20.07.2017"},
        {"client_ids": ["1", "2"], "date": "20.07.2017"},
        {"client_ids": [1, 2], "date": "XXX"},
    ])
    def test_invalid_interests_request(self, arguments):
        request = {"account": "horns&hoofs", "login": "h&f",
                   "method": "clients_interests", "arguments": arguments}
        self.set_valid_auth(request)
        response, code = self.get_response(request)
        self.assertEqual(api.INVALID_REQUEST, code)
        self.assertTrue(len(response))

if __name__ == "__main__":
    unittest.main()
