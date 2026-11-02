"""daily_health_check.sh 의 report_paper_ledger — 가짜 docker·date 로 스크립트를 그대로 돌린다(DB 불요)."""

from __future__ import annotations

import os
import stat
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
SCRIPT = REPO / "scripts" / "daily_health_check.sh"
LEDGERS = {
    "leader21": ["leader_A", "leader_C"],
    "leader_c20": ["leader_C20"],
    "leader_rs": ["leader_A_rs"],
    "leader_m": ["leader_M"],
    "leader_p": ["leader_P"],
    "leader_m_ana": ["leader_M_ana"],
}
PEAD = "pead_rank_f1_ledger.jsonl"

# SQL 을 보고 답한다: 기대 거래일 쿼리는 "date < 크론일" 을 그대로 받아 답이 크론일에 달려 있음을 시험한다.
DOCKER = r"""#!/usr/bin/env bash
sql="$*"
case "$sql" in
  *"code = '001'"*)
    [ -n "${FAKE_DB_DOWN:-}" ] && exit 1
    day=$(printf '%s' "$sql" | grep -oE "date < '[0-9-]{10}'" | grep -oE '[0-9-]{10}')
    /usr/bin/awk -F= -v d="$day" '$1 == d { print $2 }' "$FAKE_PREV_MAP" ;;
  *"FROM ticks WHERE ts >= '"*"::date + 1"*) echo "${FAKE_TICKS:-f}" ;;
  *) ;;
esac
exit 0
"""
DATE = r"""#!/usr/bin/env bash
for a in "$@"; do
  case "$a" in -d|--date*) exec /usr/bin/date "$@" ;; esac
done
exec /usr/bin/date -d "$FAKE_NOW" "$@"
"""
# 크론일 → 그 전 마지막 거래일. 2026-11-09(월)·11-06(금)·11-02(월), 휴장 가정 없음.
PREV_MAP = "2026-11-02=2026-10-30\n2026-11-03=2026-11-02\n2026-11-05=2026-11-04\n2026-11-06=2026-11-05\n2026-11-09=2026-11-06\n"


@pytest.fixture
def env(tmp_path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    for name, body in (("docker", DOCKER), ("date", DATE)):
        path = bin_dir / name
        path.write_text(body, encoding="utf-8")
        path.chmod(path.stat().st_mode | stat.S_IEXEC)
    prev = tmp_path / "prev.map"
    prev.write_text(PREV_MAP, encoding="utf-8")
    (tmp_path / "wt" / "research" / "paper").mkdir(parents=True)
    return {
        **{k: v for k, v in os.environ.items() if k not in ("ALERT_WEBHOOK_URL", "ALERT_LOG")},
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "HOME": str(tmp_path),
        "FAKE_PREV_MAP": str(prev),
        "PAPER_WORKTREE": str(tmp_path / "wt"),
        "PAPER_LOG": str(tmp_path / "paper.log"),
        "ALERT_WEBHOOK_URL": "",
        "ALERT_LOG": str(tmp_path / "alerts.log"),
        "BACKUP_OK_MARKER": str(tmp_path / "no-marker"),
    }


def paper(env) -> Path:
    return Path(env["PAPER_WORKTREE"]) / "research" / "paper"


def write_ledgers(env, day: str, skip: tuple = ()) -> None:
    for name, arms in LEDGERS.items():
        if name in skip:
            continue
        rows = [f'{{"date": "2026-10-13", "arm": "{a}", "cfg": {{"date": "2099-01-01"}}}}' for a in arms]
        rows += [f'{{"date": "{day}", "arm": "{a}", "cfg": {{}}}}' for a in arms]
        (paper(env) / f"{name}_ledger.jsonl").write_text("\n".join(rows) + "\n", encoding="utf-8")


def write_pead(env, day: str) -> None:
    (paper(env) / PEAD).write_text(f'{{"date": "{day}", "arm": "pead_rank_F1", "rules": {{}}}}\n', encoding="utf-8")


def write_log(env, day: str, pead_line: str = "시작 전: x", extra: str = "") -> None:
    Path(env["PAPER_LOG"]).write_text(
        f"== 2026-10-01 07:43:00 leader_A\n실패: 옛날\n시작 전: 옛날\n"
        f"== {day} 07:43:00 leader_A\n기록: {day} leader_A\n{extra}"
        f"== {day} 07:50:00 pead_rank_F1\n{pead_line}\n커밋 abc\n",
        encoding="utf-8",
    )


def run(env, now: str, **extra) -> list[str]:
    """모의매매 경보 줄만(다른 report_* 는 가짜 docker 라 빈 값으로 운다). 알림 통로까지 간 것도 확인한다."""
    proc = subprocess.run(["bash", str(SCRIPT)], env={**env, "FAKE_NOW": now, **extra}, capture_output=True, text=True, timeout=60)
    assert proc.returncode == 0, proc.stderr
    warns = [line for line in proc.stdout.splitlines() if "⚠️ 모의매매" in line]
    if warns:
        assert "모의매매" in Path(env["ALERT_LOG"]).read_text(encoding="utf-8")
    return warns


def test_all_fresh_is_quiet(env):
    write_ledgers(env, "2026-11-05")
    write_pead(env, "2026-11-05")
    write_log(env, "2026-11-06", "기록: 2026-11-05 pead_rank_F1")
    assert run(env, "2026-11-06 18:10:00") == []


def test_prestart_accepts_log_line(env):
    write_ledgers(env, "2026-11-04")
    write_log(env, "2026-11-05", "시작 전: 2026-11-04 < 2026-11-05 pead_rank_F1 — 쓰지 않는다")
    assert run(env, "2026-11-05 11:35:00") == []


def test_prestart_tolerance_ends_after_1135_on_first_row_day(env):
    write_ledgers(env, "2026-11-05")
    write_log(env, "2026-11-06", "시작 전: 2026-11-04 < 2026-11-05 pead_rank_F1")
    assert run(env, "2026-11-06 11:35:00") == []
    warns = run(env, "2026-11-06 18:10:00")
    assert len(warns) == 1 and "첫 행 시한" in warns[0]


def test_prestart_without_log_line_warns(env):
    """leader 팔의 '시작 전' 으로 대신하지 않는다 — pead 구간 안이어야 한다."""
    write_ledgers(env, "2026-11-02")
    write_log(env, "2026-11-03", "기록: 뭔가", extra="시작 전: leader 팔\n")
    warns = run(env, "2026-11-03 11:35:00")
    assert len(warns) == 1 and "'시작 전' 이 없다" in warns[0]


def test_stale_pead_ledger_warns(env):
    write_ledgers(env, "2026-11-06")
    write_pead(env, "2026-11-05")
    write_log(env, "2026-11-09", "이미 기록됨: 2026-11-05")
    warns = run(env, "2026-11-09 11:35:00")
    assert len(warns) == 1 and "pead_rank_F1" in warns[0] and "2026-11-06" in warns[0]


def test_stale_or_lost_leader_ledger_warns_per_arm(env):
    write_ledgers(env, "2026-11-05", skip=("leader_p",))
    (paper(env) / "leader_m_ledger.jsonl").write_text('{"date": "2026-11-04", "arm": "leader_M"}\n', encoding="utf-8")
    write_pead(env, "2026-11-05")
    write_log(env, "2026-11-06", "기록: x")
    warns = run(env, "2026-11-06 11:35:00")
    assert len(warns) == 1 and "leader_M=2026-11-04" in warns[0] and "leader_p=장부없음" in warns[0]


def test_failure_line_today_warns_but_old_failure_does_not(env):
    write_ledgers(env, "2026-11-05")
    write_pead(env, "2026-11-05")
    write_log(env, "2026-11-06", "실패: pead_rank_F1")
    warns = run(env, "2026-11-06 11:35:00")
    assert len(warns) == 1 and "실패: pead_rank_F1" in warns[0]


def test_dead_cron_no_section_today(env):
    write_ledgers(env, "2026-11-04")
    write_pead(env, "2026-11-04")
    write_log(env, "2026-11-05", "기록: x")
    warns = run(env, "2026-11-06 11:35:00")
    assert any("실행 흔적 없음" in w for w in warns)
    assert any("pead_rank_F1 장부 최신 2026-11-04" in w for w in warns)


def test_missing_log_file_warns(env):
    write_ledgers(env, "2026-11-05")
    write_pead(env, "2026-11-05")
    warns = run(env, "2026-11-06 11:35:00")
    assert len(warns) == 1 and "실행 흔적 없음" in warns[0]


def test_missing_worktree_warns(env):
    write_log(env, "2026-11-06", "기록: x")
    warns = run(env, "2026-11-06 11:35:00", PAPER_WORKTREE=env["PAPER_WORKTREE"] + "-gone")
    assert len(warns) == 1 and "워크트리 없음" in warns[0]


def test_weekend_expects_row_written_by_friday_cron(env):
    """토요일: 금요일 크론은 목요일 행을 썼다 — 금요일을 기대하면 매주 오보다. 로그도 안 본다."""
    write_ledgers(env, "2026-11-05")
    write_pead(env, "2026-11-05")
    assert run(env, "2026-11-07 11:35:00") == []
    write_ledgers(env, "2026-11-04")
    write_pead(env, "2026-11-04")
    assert len(run(env, "2026-11-08 18:10:00")) == 2


def test_db_down_warns(env):
    write_ledgers(env, "2026-11-05")
    write_log(env, "2026-11-06", "기록: x")
    warns = run(env, "2026-11-06 11:35:00", FAKE_DB_DOWN="1")
    assert len(warns) == 1 and "조회 실패" in warns[0]


def test_whole_calendar_stuck_is_caught_by_ticks(env):
    """이미 기록됨 + 지수도 멈춤: 장부는 기대와 같아 조용하다 — 그 뒤 틱이 있으면 운다."""
    write_ledgers(env, "2026-11-05")
    write_pead(env, "2026-11-05")
    write_log(env, "2026-11-06", "이미 기록됨: 2026-11-05")
    warns = run(env, "2026-11-06 11:35:00", FAKE_TICKS="t")
    assert len(warns) == 1 and "틱이 있다" in warns[0]
