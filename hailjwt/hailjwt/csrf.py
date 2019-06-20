import secrets
import logging
from functools import wraps
from aiohttp import web

log = logging.getLogger('hailjwt')


def new_csrf_token():
    return secrets.token_bytes(64)


def check_csrf_token(fun):
    @wraps(fun)
    def wrapped(request, *args, **kwargs):
        token1 = request.cookies.get('_csrf')
        token2 = request.query.get('_csrf')
        if token1 is not None and token2 is not None and token1 == token2:
            return fun(request, *args, **kwargs)

        log.info('request made with invalid csrf tokens')
        raise web.HTTPForbidden()
    return wrapped
