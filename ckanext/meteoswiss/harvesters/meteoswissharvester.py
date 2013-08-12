#n -*- coding: utf-8 -*-



from ckanext.harvest.model import HarvestJob
from ckanext.harvest.harvesters import HarvesterBase

from ckanext.swisstopo.helpers import s3

import logging
log = logging.getLogger(__name__)

class MeteoswissHarvester(HarvesterBase):
    '''
    The harvester for meteoswiss
    '''



    def info(self):
        return {
            'name': 'meteoswiss',
            'title': 'METEOSWISS',
            'description': 'Harvests the meteoswiss data',
            'form_config_interface': 'Text'
        }

    def gather_stage(self, harvest_job):
        pass


    def fetch_stage(self, harvest_object):
        pass

    def import_stage(self, harvest_object):
        pass
