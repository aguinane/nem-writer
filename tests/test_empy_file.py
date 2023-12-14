
import pytest

from nemwriter import NEM12, NEM13


def test_empty_nem12_file():
    """should error if file would be empty"""
    m = NEM12(to_participant="123")
    output_file = "tests/empty.zip"
    with pytest.raises(ValueError):
        m.output_zip(output_file)


def test_empty_nem13_file():
    """should error if file would be empty"""
    m = NEM13(to_participant="123")
    output_file = "tests/empty.zip"
    with pytest.raises(ValueError):
        m.output_zip(output_file)
