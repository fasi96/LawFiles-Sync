# Vendored copy of the LawFiles BCI converter

Source repo: https://github.com/fasi96/LawFiles

Only `src/` and `config/` (field_mapping.json + defaults.json) are included —
those are what `lib/converter.py` imports and reads at runtime.

We vendor (instead of using a git submodule) because Vercel's serverless
function bundler does not reliably include submodule contents in the Lambda
filesystem — see the pipeline incident from the first prod deploy.

## To update the vendored copy

```bash
# in a separate clone of the converter
cd /path/to/LawFiles
git pull

# back here
rm -rf vendor/LawFiles/src vendor/LawFiles/config
mkdir -p vendor/LawFiles/src vendor/LawFiles/config
cp -r /path/to/LawFiles/src/*           vendor/LawFiles/src/
cp /path/to/LawFiles/config/field_mapping.json  vendor/LawFiles/config/
cp /path/to/LawFiles/config/defaults.json       vendor/LawFiles/config/
rm -rf vendor/LawFiles/src/__pycache__

# verify import still works
python -c "from lib import converter; converter.load_converter_config()"

git add vendor/LawFiles
git commit -m "Update vendored LawFiles converter to <upstream-sha>"
```
