import py4j


class FatalError(Exception):
    """:class:`.FatalError` is an error thrown by Hail method failures"""


class Env:
    _jvm = None
    _gateway = None
    _hail_package = None
    _jutils = None
    _hc = None

    @staticmethod
    def jvm():
        if not Env._jvm:
            raise EnvironmentError('no Hail context initialized, create one first')
        return Env._jvm

    @staticmethod
    def hail():
        if not Env._hail_package:
            Env._hail_package = getattr(Env.jvm(), 'is').hail
        return Env._hail_package

    @staticmethod
    def gateway():
        if not Env._gateway:
            raise EnvironmentError('no Hail context initialized, create one first')
        return Env._gateway

    @staticmethod
    def jutils():
        if not Env._jutils:
            Env._jutils = scala_package_object(Env.hail().utils)
        return Env._jutils

    @staticmethod
    def hc():
        if not Env._hc:
            raise EnvironmentError('no Hail context initialized, create one first')
        return Env._hc


def jarray(jtype, lst):
    jarr = Env.gateway().new_array(jtype, len(lst))
    for i, s in enumerate(lst):
        jarr[i] = s
    return jarr


def scala_object(jpackage, name):
    return getattr(getattr(jpackage, name + '$'), 'MODULE$')


def scala_package_object(jpackage):
    return scala_object(jpackage, 'package')


def jnone():
    return scala_object(Env.jvm().scala, 'None')


def jsome(x):
    return Env.jvm().scala.Some(x)


def joption(x):
    return jsome(x) if x else jnone()


def from_option(x):
    return x.get() if x.isDefined() else None


def jindexed_seq(x):
    return Env.jutils().arrayListToISeq(x)

def jset(x):
    return Env.jutils().arrayListToSet(x)

def jindexed_seq_args(x):
    args = [x] if isinstance(x, str) else x
    return jindexed_seq(args)

def jset_args(x):
    args = [x] if isinstance(x, str) else x
    return jset(args)


def jiterable_to_list(it):
    if it:
        return list(Env.jutils().iterableToArrayList(it))
    else:
        return None


def jarray_to_list(a):
    return list(a) if a else None


<<<<<<< HEAD
@decorator
def handle_py4j(func, *args, **kwargs):
    try:
        r = func(*args, **kwargs)
    except Py4JJavaError as e:
        msg = Env.jutils().getMinimalMessage(e.java_exception)
        raise FatalError(msg)
    except Py4JError as e:
        Env.jutils().log().error('hail: caught python exception: ' + str(e))
        if e.args[0].startswith('An error occurred while calling'):
            raise TypeError('Method %s() received at least one parameter with an invalid type. '
                            'See doc for function signature.' % func.__name__)
        else:
            raise e
    return r
=======
def capture_py4j_exception(f):
    def deco(*a, **kw):
        try:
            return f(*a, **kw)
        except py4j.protocol.Py4JJavaError as e:
            tpl = env.jutils.handleForPython(e.java_exception)
            deepest, full = tpl._1(), tpl._2()

            raise FatalError('%s\n\nJava stack trace:\n%s\n\nERROR SUMMARY: %s' % (deepest, full, deepest))
        except py4j.protocol.Py4JError as e:
            if e.args[0].startswith('An error occurred while calling'):
                msg = 'An error occurred while calling into JVM, probably due to invalid parameter types'
                raise FatalError('%s\n\nJava stack trace:\n%s\n\nERROR SUMMARY: %s' % (msg, e.message, msg))

    return deco


def install_exception_handler():
    original = py4j.protocol.get_return_value
    patched = capture_py4j_exception(original)
    py4j.protocol.get_return_value = patched
    py4j.java_gateway.get_return_value = patched


install_exception_handler()
>>>>>>> Better error checking
