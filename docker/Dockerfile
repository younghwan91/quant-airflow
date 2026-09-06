# Collectors (./collectors, bind-mounted like dags/scripts) are self-contained
# and no longer need kr-quant installed — see docker/entrypoint-wrapper.sh and
# collectors/storage.py. kr-quant is still bind-mounted at
# /opt/kr-quant for the 1 DAG that intentionally runs its analysis code in-place
# (weekly_price_adjust.py), reached via PYTHONPATH/sys.path rather than a
# package install.
FROM apache/airflow:2.10.5-python3.11

# 베이스 이미지에 git이 없다(2026-09-06 실측) — requirements.txt의
# krx-news-client/krx-fundamentals-client가 git+https URL이라 pip install이
# git을 실행 못 해 "Permission denied: 'git'"로 죽는다. 예전엔 우연히 캐시된
# 레이어로 넘어갔을 뿐, requirements.txt를 조금이라도 건드리면(예: LLM SDK
# 교체) 그 캐시가 깨져 이미지를 처음부터 재빌드할 수 없었다.
USER root
RUN apt-get update && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*
USER airflow

COPY docker/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

COPY docker/entrypoint-wrapper.sh /entrypoint-wrapper.sh
USER root
RUN chmod +x /entrypoint-wrapper.sh
USER airflow

ENTRYPOINT ["/entrypoint-wrapper.sh"]
