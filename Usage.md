1. Make sure symbols.txt exists (it does)
2. Test run with only 8 symbols (1-2 GB download max)
```python
python process_data.py --max-symbols 8
```
3. After it finishes and you confirm parquet is good:
```python
python process_data.py --max-symbols 80   # full universe (will reuse already downloaded files)
```