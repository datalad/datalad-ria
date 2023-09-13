from __future__ import annotations


def build_ria_url(
    protocol: str,
    host: str,
    port: str | None = None,
    path: str | None = None,
    user: str | None = None,
    passwd: str | None = None,

):
    if passwd and (user is None):
        raise ValueError('password without user name given')

    url = (
        'ria+{protocol}://{user}{passdlm}{passwd}{userdlm}'
        '{host}{portdlm}{port}{pathdlm}{path}'
    ).format(
        protocol=protocol,
        user=user or '',
        passdlm=':' if passwd else '',
        passwd=passwd or '',
        userdlm='@' if user else '',
        host=host or '',
        portdlm=':' if port else '',
        port=port or '',
        pathdlm='/' if path and not path.startswith('/') else '',
        path=path or '',
    )
    return url
