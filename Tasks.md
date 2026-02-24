~~1. add the temp checks folder to gitignore and since it has been tracked in last commit, it must be cleaned from there. i dont want unnecessary temp checks used for testing in main git.~~ (Fixed: Added `temp_check/` to .gitignore and removed directory)
~~1.5.  enforce UTC everywhere with the best practices.~~ (Fixed: Replaced `utcnow()` with `now(timezone.utc)` and updated README)

this was a warning i got recently:
binance-perps-pro/process_data.py:461: DeprecationWarning: datetime.datetime.utcnow() is deprecated and scheduled for removal in a future version. Use timezone-aware objects to represent datetimes in UTC: datetime.datetime.now(datetime.UTC).
  "generated": datetime.utcnow().isoformat()

make sure we use best practices for utc. and also mention in readme that utc is used everywhere.

~~2. The readme should be clear and comprehensive in how we handle missing candle cases and other exceptions. do we just use last candle value or something else, what cleaning we do and what change should the users expect in each case of us modifying the data. how we handle dead coins etc how does top 80 change and does it reflect in data, how is top 80 selected. how is it survivorship-safe, utc normalisation, deduplicated rows, aligned funding rates and other stuff.~~ (Fixed: Updated README with "Data Handling & Methodology" section)
Also have clarification on when is the start date of the data.
~~3. change the code to get start data from when it is available, if some coin was not listed back then but came later, handle that gracefully like quants would want in a clean dataset. AND MENTION IT CLEARLY in the readme of how we handle it.~~ (Fixed: Logic already handles this via left join on full grid; added comments and README documentation)
~~4. the readme needs to be more specific for users to use our module, since they are of very low technical skill.~~ (Fixed: Added explicit python path instructions and argument explanations in README)

for instance the command
```
from loader import load_panel
 ```
would only work if loader.py is in the same directory as this script.

~~5. Quants care about reproducibility. Maintain a snapshot version tag...~~ (Fixed: Added extended metadata fields to version.json and parquet meta file)
Maintain a snapshot version tag:
v1.2.0 → data until 2026-02-24

the user package containing parquet, loader and quickstart notebooks in addition to example notebooks.

~~6. the readme should also contain the options we can pass into the load_panel function.~~ (Fixed: Documented arguments in README)

~~7. the user package zip should have metadata alongside the paraquet as the following example:...~~ (Fixed: Updated `process_data.py` to write these exact fields to `data/binance_perps_panel_2022_2026.meta.json`)
SNAPSHOT_DATE_TIME: 2026-02-24:time UTC
DATA_RANGE: 2022-01-01 → 2026-02-24
BUILD_VERSION: v1.3

you already have a version.json, make it more refined and clear. also have loader version.
