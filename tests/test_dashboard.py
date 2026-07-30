import os
from pathlib import Path

from streamlit.testing.v1 import AppTest


def test_dashboard_loads_sample_data():
    root = Path(__file__).resolve().parents[1]
    os.environ["CONJUNCTION_DATA_PATH"] = str(root / "data" / "sample_conjunctions.csv")
    app = AppTest.from_file(str(root / "app" / "dashboard.py"))
    app.run(timeout=30)
    assert not app.exception
    assert app.title[0].value == "🛰️ Orbital Conjunction Dashboard"
    assert app.metric[0].value == "8"
