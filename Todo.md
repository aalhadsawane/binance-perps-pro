

# ⚠️ Minor Issues to fix (Not Required Changes)

⸻

binance-perps-pro/process_data.py:461: DeprecationWarning: datetime.datetime.utcnow() is deprecated and scheduled for removal in a future version. Use timezone-aware objects to represent datetimes in UTC: datetime.datetime.now(datetime.UTC).
  "generated": datetime.utcnow().isoformat()

🟢 Performance Expectations

Fast test (recommended)

--max-symbols 2 --end-date 2022-02-01

⏱ ~30–60 sec

⸻

Full 2 symbols

⏱ 2–4 min first run
⏱ seconds after cache

⸻

Full 80 symbols

⏱ 35–70 minutes first run
⏱ minutes rebuild

---