import functools
import hashlib
from datetime import datetime

from api import api

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


def set_valid_auth(request):
        if request.get("login") == api.ADMIN_LOGIN:
            d = datetime.now().strftime("%Y%m%d%H") + api.ADMIN_SALT
            request["token"] = hashlib.sha512(d.encode('utf-8')).hexdigest()
        else:
            msg = request.get("account", "") + request.get("login", "") + api.SALT
            request["token"] = hashlib.sha512(msg.encode('utf-8')).hexdigest()