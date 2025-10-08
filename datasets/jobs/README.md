# Jobs datasets

Scripts and data related to Human Resources job family occupations collected from the O*NET beta web services.

## Fetching

1. Request beta API credentials from O*NET.
2. Export them before running the fetcher:
   ```bash
   export ONET_USER="antoniomoneo@gmail.com"
   export ONET_KEY="your_api_key_here"
   ```
3. Install dependencies if needed:
   ```bash
   pip install requests
   ```
4. Run the fetch script (defaults write into `data/`):
   ```bash
   python fetch_onet_hr_family.py
   ```

The script calls `mnm/occupations` and `mnm/occupations/{code}` from `services-beta.onetcenter.org`, groups Human Resources roles into variants (compensation, business partner, talent acquisition, and others) and stores both JSON and CSV outputs under `data/`.

## Outputs

- `data/human_resources_buckets.json`: grouped raw payloads keyed by variant.
- `data/human_resources_buckets.csv`: flat table with code, title, job family and description.

Re-run the fetcher whenever O*NET refreshes their beta data snapshots.
