from __future__ import annotations


def test_pattern_filter_simple():
    from ..sshshell import PatternFilter

    pf = PatternFilter(b'end-mark-1')
    d = pf.filter(b'nd-mark-')
    assert d == (b'nd-mark-', False, b'')
    d = pf.filter(b'abcend-mark-1')
    assert d == (b'abc', True, b'')
    d = pf.filter(b'abcend-mark-1def')
    assert d == (b'abc', True, b'def')


def test_pattern_filter_basic():
    from ..sshshell import PatternFilter

    pf = PatternFilter(b'end-mark-1')
    d = pf.filter(b'aend-')
    assert d == (b'a', False, b'')
    d = pf.filter(b'mark-')
    assert d == (b'', False, b'')
    d = pf.filter(b'1')
    assert d == (b'', True, b'')


def test_pattern_filter_2():
    from ..sshshell import PatternFilter

    pf = PatternFilter(b'end-mark-1')
    d = pf.filter(b'aend-')
    assert d == (b'a', False, b'')
    d = pf.filter(b'mark-')
    assert d == (b'', False, b'')
    d = pf.filter(b'11')
    assert d == (b'', True, b'1')
