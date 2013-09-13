ckanext-meteoswiss
==================

Harvester for the Federal Office of Meteorology and Climatology MeteoSwiss

## Installation

Use `pip` to install this plugin. This example installs it in `/home/www-data`

```bash
source /home/www-data/pyenv/bin/activate
pip install -e git+https://github.com/ogdch/ckanext-meteoswiss.git#egg=ckanext-meteoswiss --src /home/www-data
cd /home/www-data/ckanext-meteoswiss
pip install -r pip-requirements.txt
python setup.py develop
```

Make sure to add `meteoswiss_harvest` to `ckan.plugins` in your config file.

## Run harvester

```bash
source /home/www-data/pyenv/bin/activate
paster --plugin=ckanext-meteoswiss meteoswiss_harvest gather_consumer -c development.ini &
paster --plugin=ckanext-meteoswiss meteoswiss_harvest fetch_consumer -c development.ini &
paster --plugin=ckanext-meteoswiss meteoswiss_harvest run -c development.ini
```
