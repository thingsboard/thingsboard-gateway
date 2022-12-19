import multiprocessing
from multiprocessing import connection, process
from multiprocessing.managers import dispatch, convert_to_error


class GatewayProxy(multiprocessing.managers.BaseProxy):
    def _callmethod(self, methodname, args=(), kwds={}):
        """
        Try to call a method of the referent and return a copy of the result
        """
        self._id = list(self._idset)[0]
        self._connect()
        conn = self._tls.connection

        conn.send((self._id, methodname, args, kwds))
        kind, result = conn.recv()

        if kind == '#RETURN':
            return result
        elif kind == '#PROXY':
            exposed, token = result
            proxytype = self._manager._registry[token.typeid][-1]
            token.address = self._token.address
            proxy = proxytype(
                token, self._serializer, manager=self._manager,
                authkey=self._authkey, exposed=exposed
            )
            conn = self._Client(token.address, authkey=self._authkey)
            dispatch(conn, None, 'decref', (token.id,))
            return proxy
        raise convert_to_error(kind, result)


def AutoProxy(token, serializer, manager=None, authkey=None,
              exposed=None, incref=True, manager_owned=False):
    """
    Return an auto-proxy for `token`
    """
    _Client = connection.Client

    if exposed is None:
        conn = _Client(token.address, authkey=authkey)
        try:
            exposed = dispatch(conn, None, 'get_methods', (token,))
        finally:
            conn.close()

    if authkey is None and manager is not None:
        authkey = manager._authkey
    if authkey is None:
        authkey = process.current_process().authkey

    ProxyType = MakeProxyType('AutoProxy[%s]' % token.typeid, exposed)
    proxy = ProxyType(token, serializer, manager=manager, authkey=authkey,
                      incref=incref, manager_owned=manager_owned)
    proxy._isauto = True
    return proxy


def MakeProxyType(name, exposed, _cache={}):
    """
    Return a proxy type whose methods are given by `exposed`
    """
    exposed = tuple(exposed)
    try:
        return _cache[(name, exposed)]
    except KeyError:
        pass

    dic = {}

    for meth in exposed:
        exec('''def %s(self, /, *args, **kwds):
        return self._callmethod(%r, args, kwds)''' % (meth, meth), dic)

    ProxyType = type(name, (GatewayProxy,), dic)
    ProxyType._exposed_ = exposed
    _cache[(name, exposed)] = ProxyType
    return ProxyType