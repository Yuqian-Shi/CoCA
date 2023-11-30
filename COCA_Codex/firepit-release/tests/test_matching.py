from firepit.query import Filter, Predicate, Query, Table

from .helpers import tmp_storage


def test_like(one_event_bundle, tmpdir):
    store = tmp_storage(tmpdir)
    store.cache('q1', one_event_bundle)

    # Match using STIX pattern and LIKE operator
    store.extract(
        'x',
        'artifact',
        'q1',
        "[artifact:payload_bin LIKE '%IngressInterface=ethernet1/1%']",
    )
    x = store.lookup('x')
    assert len(x) == 1
    store.extract(
        'y',
        'artifact',
        'q1',
        "[artifact:payload_bin LIKE '%IngressInterface=ethernet1/2%']",
    )
    y = store.lookup('y')
    assert len(y) == 0

    # Match using SQL query and LIKE operator
    qry = Query(
        [
            Table('artifact'),
            Filter(
                [Predicate('payload_bin', 'LIKE', '%IngressInterface=ethernet1/1%')]
            ),
        ]
    )
    s = store.run_query(qry).fetchall()
    print(s)
    assert len(s) == 1


def test_matches(one_event_bundle, tmpdir):
    store = tmp_storage(tmpdir)
    store.cache('q1', one_event_bundle)

    # Match using STIX pattern and LIKE operator
    store.extract(
        'x',
        'artifact',
        'q1',
        r"[artifact:payload_bin MATCHES '(Ing|E)ressInterface=ethernet1/\d']",
    )
    x = store.lookup('x')
    assert len(x) == 1
    store.extract(
        'y',
        'artifact',
        'q1',
        "[artifact:payload_bin MATCHES '(Ing|E)ressInterface=ethernet1/2']",
    )
    y = store.lookup('y')
    assert len(y) == 0


def test_matches_regkey(regkey_bundle, tmpdir):
    store = tmp_storage(tmpdir)
    store.cache('q1', regkey_bundle)

    # Match using STIX pattern and MATCHES operator
    store.extract(
        'x',
        'windows-registry-key',
        'q1',
        r"[windows-registry-key:key MATCHES '^.*\\\\Microsoft\\\\Windows\\\\CurrentVersion\\\\Run(Once)?$']",
    )
    x = store.lookup('x')
    assert len(x) == 1
