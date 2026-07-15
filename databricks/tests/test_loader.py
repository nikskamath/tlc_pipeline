import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from load_to_databricks import get_target_month, LATEST_AVAILABLE


def test_get_target_month_override():
    assert get_target_month("2024-01") == "2024-01"


def test_get_target_month_default():
    month = get_target_month()
    assert month == LATEST_AVAILABLE
    assert len(month) == 7
    assert month[4] == "-"


def test_month_format():
    month = get_target_month("2024-06")
    year, mon = month.split("-")
    assert len(year) == 4
    assert len(mon) == 2
    assert 1 <= int(mon) <= 12
