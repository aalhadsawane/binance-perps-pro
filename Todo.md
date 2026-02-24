

# ⚠️ Minor Notes (Not Required Changes)

⸻

2️⃣ Cache invalidation (future)

If you change:
	•	schema
	•	features
	•	logic

old cache persists.

If needed later:

cache/v1_3_BTCUSDT.parquet

Not urgent.

⸻

3️⃣ Funding column null bursts

Funding occurs every 8h → nulls expected.

This is correct.

⸻

4️⃣ Memory profile

With 80 symbols:
	•	RAM ~ 2–3 GB peak
	•	safe on 8GB+

⸻

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