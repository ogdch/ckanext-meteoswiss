from setuptools import setup, find_packages
import sys, os

version = '0.0'

setup(
	name='ckanext-meteoswiss',
	version=version,
	description="CKAN extension of the Federal Office of Meteorology and Climatology for the OGD portal of Switzerland",
	long_description="""\
	""",
	classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
	keywords='',
	author='Liip AG',
	author_email='ogd@liip.ch',
	url='http://www.liip.ch',
	license='GPL',
	packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
	namespace_packages=['ckanext', 'ckanext.meteoswiss'],
	include_package_data=True,
	zip_safe=False,
	install_requires=[
		# -*- Extra requirements: -*-
	],
	entry_points=\
	"""
    [ckan.plugins]
	#meteoswiss_plugin=ckanext.meteoswiss:PluginClass
    meteoswiss_harvest=ckanext.meteoswiss.harvesters:MeteoswissHarvester
    [paste.paster_command]
    meteoswiss_harvest=ckanext.meteoswiss.commands.harvester:Harvester
	""",
)
