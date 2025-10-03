# Sync Case Data to Elasticsearch

## Steps

1. Set hash key in Redis:
   ```
   elasticindex:cases
   ```
   
   With values:
   ```
   [caseid,'P'], [caseid,'P']
   ```

2. Run the Python indexing script:
   ```bash
   python3 index-uuid.py
   ```

---

# Sync View Summary Data

## Steps

1. Run the Python summary indexing script:
   ```bash
   python3 index_summary.py
   ```