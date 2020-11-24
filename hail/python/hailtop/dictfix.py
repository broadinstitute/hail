class NoneOr:
    def __init__(self, subspec):
        self.subspec = subspec


def dictfix(x, spec):
    assert isinstance(x, dict) or x is None
    assert isinstance(spec, dict) or isinstance(spec, NoneOr)
    return _dictfix(x, spec)


def _dictfix(x, spec):
    if isinstance(spec, NoneOr):
        if x is not None:
            x = _dictfix(x, spec.subspec)
    elif isinstance(spec, type):
        assert isinstance(x, spec), f'{x!r} should be {spec!r}'
    elif isinstance(spec, dict):

        if x is None:
            x = dict()
        for k, subspec in spec.items():
            v = None
            if k in x:
                v = x.get(k)
            try:
                x[k] = _dictfix(v, subspec)
            except AssertionError as err:
                raise AssertionError(f'{k}:{v}', err)
    else:
        if x is None:
            x = spec
    return x
