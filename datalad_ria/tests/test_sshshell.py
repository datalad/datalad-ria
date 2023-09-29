from __future__ import annotations

from ..sshshell import PatternFilter


def test_pattern_filter_simple():
    pf = PatternFilter(b'end-mark-1')
    d = pf.filter(b'nd-mark-')
    assert d == (b'nd-mark-', False, b'')
    d = pf.filter(b'abcend-mark-1')
    assert d == (b'abc', True, b'')
    d = pf.filter(b'abcend-mark-1def')
    assert d == (b'abc', True, b'def')


def test_pattern_filter_basic():
    pf = PatternFilter(b'end-mark-1')
    d = pf.filter(b'aend-')
    assert d == (b'a', False, b'')
    d = pf.filter(b'mark-')
    assert d == (b'', False, b'')
    d = pf.filter(b'1')
    assert d == (b'', True, b'')


def test_pattern_filter_2():
    pf = PatternFilter(b'end-mark-1')
    d = pf.filter(b'aend-')
    assert d == (b'a', False, b'')
    d = pf.filter(b'mark-')
    assert d == (b'', False, b'')
    d = pf.filter(b'11')
    assert d == (b'', True, b'1')


def test_bug_newline():
    pattern = b'datalad-result-601405900:'
    pf = PatternFilter(pattern)
    d = pf.filter(pattern + b'2\n')
    assert d == (pattern, True, b'2\n')
