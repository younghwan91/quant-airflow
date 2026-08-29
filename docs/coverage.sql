\pset border 2
\pset linestyle ascii
\pset numericlocale on
SELECT '일봉' AS "데이터", to_char(count(*),'FM999,999,999') AS "행 수", min(date)::text AS "시작", max(date)::text AS "끝" FROM daily_bars
UNION ALL SELECT '일봉 · 수정주가', to_char(count(*),'FM999,999,999'), min(date)::text, max(date)::text FROM daily_bars_adjusted
UNION ALL SELECT '투자자 수급', to_char(count(*),'FM999,999,999'), min(date)::text, max(date)::text FROM supply_demand
UNION ALL SELECT '공매도', to_char(count(*),'FM999,999,999'), min(date)::text, max(date)::text FROM short_selling
UNION ALL SELECT '신용잔고', to_char(count(*),'FM999,999,999'), min(date)::text, max(date)::text FROM credit_balance
UNION ALL SELECT '업종지수', to_char(count(*),'FM999,999,999'), min(date)::text, max(date)::text FROM sector_index
UNION ALL SELECT '상장주식수', to_char(count(*),'FM999,999,999'), min(date)::text, max(date)::text FROM shares_outstanding_history
UNION ALL SELECT '실적(DART)', to_char(count(*),'FM999,999,999'), min(avail_date)::text, max(avail_date)::text FROM earnings
UNION ALL SELECT '컨센서스', to_char(count(*),'FM999,999,999'), min(date)::text, max(date)::text FROM consensus;

SELECT '상장 종목' AS "유니버스", to_char(count(*),'FM999,999,999') AS "종목 수" FROM stocks
UNION ALL SELECT '상장폐지 종목', to_char(count(*),'FM999,999,999') FROM delisted_stocks;
