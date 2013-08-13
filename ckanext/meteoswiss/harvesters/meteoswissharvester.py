#n -*- coding: utf-8 -*-
import xlrd
import json

from boto.s3.connection import S3Connection
from boto.s3.key import Key

from ckan import model
from ckan.model import Session

from ckanext.harvest.model import HarvestJob, HarvestObject
from ckanext.harvest.harvesters import HarvesterBase

from pylons import config

import logging
log = logging.getLogger(__name__)

class MeteoswissHarvester(HarvesterBase):
    '''
    The harvester for meteoswiss
    '''

    HARVEST_USER = u'harvest'

    METADATA_FILE_NAME = u'OGD@Bund_Metadaten_MeteoSchweiz.xlsx'
    FILES_BASE_URL = 'http://opendata-ch.s3.amazonaws.com'

    BUCKET_NAME = u'opendata-ch'
    AWS_ACCESS_KEY = config.get('ckanext.meteoswiss.access_key')
    AWS_SECRET_KEY = config.get('ckanext.meteoswiss.secret_key')

    ROW_TYPES = [
        'ckan_entity',
        'ckan_attribute',
        'ckan_description',
        'gm03_source',
        'gm03_description',
        'cardinality',
        'value_de',
        'value_fr',
        'value_it',
        'value_en',
    ]

    DATASET_IDS = {
        'Webcams': 'ch.meteoschweiz.kamerabilder',
    }

    def _get_s3_bucket(self):
        '''
        Create an S3 connection to the department bucket
        '''
        log.debug(self.AWS_ACCESS_KEY)
        log.debug(self.AWS_SECRET_KEY)
        conn = S3Connection(self.AWS_ACCESS_KEY, self.AWS_SECRET_KEY)
        bucket = conn.get_bucket(self.BUCKET_NAME)
        return bucket


    def _fetch_metadata_file(self):
        '''
        Fetching the Excel metadata file for the SFA from the S3 Bucket and save on disk
        '''
        try:
            metadata_file = Key(self._get_s3_bucket())
            metadata_file.key = self.METADATA_FILE_NAME
            metadata_file.get_contents_to_filename(self.METADATA_FILE_NAME)
        except Exception, e:
            log.exception(e)

    def _get_row_dict_array(self, sheet_name):
        '''
        Retruns all rows from a sheet as dict
        '''
        try:
            wb = xlrd.open_workbook(self.METADATA_FILE_NAME)
            worksheet = wb.sheet_by_name(sheet_name)

            rows = []
            for row_num in range(1, worksheet.nrows):
                row_values = worksheet.row_values(row_num)
                row_values_clean = self._clean_row_values(row_values)
                row_data = dict(zip(self.ROW_TYPES, row_values_clean))
                rows.append(row_data)

            return rows
        except Exception:
            return []

    def _clean_row_values(self, row_values):
        '''
        Strip whitespace from all string
        '''
        cleaned = []
        for value in row_values:
            if isinstance(value, basestring):
                value = value.strip()

            cleaned.append(value)

        return cleaned


    def _organize_webcam_data(self, rows):
        '''
        Seperates dataset and resrouce values
        '''
        dataset = {}
        resources = []

        for row in rows:
            if row.get('ckan_entity') == 'Dataset':
                if row.get('value_de'):
                    dataset[row.get('ckan_attribute')] = row.get('value_de')

            if row.get('ckan_entity') == 'Resource':
                resources.append(row)

        dataset_id = self.DATASET_IDS.get(dataset.get('id'))

        dataset['resources'] = self._create_webcam_resources(resources, dataset_id)
        return dataset


    def _create_webcam_resources(self, resources, dataset):
        '''
        Fake resource building for webcam images
        '''
        data = []
        for row in resources:
            url = '{base_url}/{dataset}/{file}.jpg'.format(
                base_url = self.FILES_BASE_URL,
                dataset = dataset,
                file = row.get('value_de')
            )

            data.append({
                'url': url,
                'name': row.get('gm03_description'),
                'format': 'jpg',
            })

        return data

    def info(self):
        return {
            'name': 'meteoswiss',
            'title': 'Meteoswiss',
            'description': 'Harvests the meteoswiss data',
            'form_config_interface': 'Text'
        }

    def gather_stage(self, harvest_job):
        log.debug('In Meteoswiss gather_stage')

        self._fetch_metadata_file()
        rows = self._get_row_dict_array('Kamerabild')
        webcam_data = self._organize_webcam_data(rows)

        obj = HarvestObject(
            guid = webcam_data.get('id'),
            job = harvest_job,
            content = json.dumps(webcam_data)
        )
        obj.save()

        return [obj.id,]

    def fetch_stage(self, harvest_object):
        log.debug('In Meteoswiss fetch_stage')
        harvest_object.save()

        return True

    def import_stage(self, harvest_object):
        log.debug('In Meteoswiss import_stage')

        if not harvest_object:
            log.error('No harvest object received')
            return False

        try:
            package_dict = json.loads(harvest_object.content)

            user = model.User.get(self.HARVEST_USER)

            package = model.Package.get(package_dict['id'])
            model.PackageRole(package=package, user=user, role=model.Role.ADMIN)

            log.debug('Save or update package %s' % (package_dict['name'],))
            result = self._create_or_update_package(package_dict, harvest_object)

            Session.commit()
        except Exception, e:
            log.exception(e)
        return True

