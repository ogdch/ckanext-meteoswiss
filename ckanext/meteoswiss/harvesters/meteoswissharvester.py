#n -*- coding: utf-8 -*-

import random
import os
import shutil
import tempfile
import zipfile
from pprint import pprint
from collections import defaultdict

from ckan.lib.base import c
from ckan import model
from ckan.model import Session, Package
from ckan.logic import ValidationError, NotFound, get_action, action
from ckan.lib.helpers import json

from ckanext.harvest.model import HarvestJob, HarvestObject, HarvestGatherError, HarvestObjectError
from base import OGDCHHarvesterBase

from ckanext.swisstopo.helpers import ckan_csw
from ckanext.swisstopo.helpers import s3

import logging
log = logging.getLogger(__name__)

class MeteoswissHarvester(OGDCHHarvesterBase):
    '''
    The harvester for meteoswiss
    '''

    def info(self):
        return {
            'name': 'meteoswiss',
            'title': 'meteoswiss',
            'description': 'Harvests the meteoswiss data',
            'form_config_interface': 'Text'
        }

    def gather_stage(self, harvest_job):
        pass


    def fetch_stage(self, harvest_object):
        pass

    def import_stage(self, harvest_object):
        pass
