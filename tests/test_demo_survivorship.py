"""scripts/demo_survivorship.py — 합성 데이터에서 폐지 종목이 빠지면 수익률이 부풀려진다."""

import importlib.util
from pathlib import Path

from collectors.storage import connect

_spec = importlib.util.spec_from_file_location(
    "demo_survivorship", Path(__file__).resolve().parent.parent / "scripts" / "demo_survivorship.py")
demo = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(demo)


def test_survivors_only_overstates_return(tmp_path, capsys):
    con = connect(tmp_path / "d.db")
    demo.build(con)
    alive = [r[0] for r in con.execute("SELECT code FROM stocks")]
    dead = [r[0] for r in con.execute("SELECT code FROM delisted_stocks")]
    assert len(alive) == 4 and len(dead) == 3
    assert demo.mean_return(con, alive) > demo.mean_return(con, alive + dead)
    demo.main()
    assert "생존편향" in capsys.readouterr().out
