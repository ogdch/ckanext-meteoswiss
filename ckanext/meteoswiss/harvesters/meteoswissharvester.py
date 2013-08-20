#n -*- coding: utf-8 -*-
import xlrd
import json
import os
from uuid import NAMESPACE_OID, uuid4, uuid5

from boto.s3.connection import S3Connection
from boto.s3.key import Key

from ckan import model
from ckan.model import Session
from ckan.logic import get_action, action

from ckanext.harvest.model import HarvestJob, HarvestObject
from ckanext.harvest.harvesters import HarvesterBase
from pylons import config

from ..helpers.metadata import MetaDataParser

import logging
log = logging.getLogger(__name__)

class MeteoswissHarvester(HarvesterBase):
    '''
    The harvester for meteoswiss
    '''

    HARVEST_USER = u'harvest'

    METADATA_FILE_NAME = u'OGD@Bund_Metadaten_MeteoSchweiz.xlsx'

    BUCKET_NAME = u'opendata-ch'

    AWS_ACCESS_KEY = config.get('ckanext.meteoswiss.access_key')
    AWS_SECRET_KEY = config.get('ckanext.meteoswiss.secret_key')

    SHEETS = (
        # Sheet name        # Use GM03 descriptions
        (u'SMN',            False),
        (u'SMN 3',          False),
        (u'Föhnindex',      False),
        (u'HomogeneDaten',  False),
        (u'Klimanormwerte', True),
        (u'Kamerabild',     True),
    )

    S3_PREFIXES = {
        u'SMN':             'ch.meteoschweiz.swissmetnet',
        u'SMN 3':           'ch.meteoschweiz.swissmetnet',
        u'Föhnindex':       'ch.meteoschweiz.swissmetnet-foehnindex',
        u'HomogeneDaten':   'ch.meteoschweiz.homogenereihen',
        u'Klimanormwerte':  'ch.meteoschweiz.normwerttabellen',
        u'Kamerabild':      'ch.meteoschweiz.kamerabilder',
    }

    ORGANIZATION = {
        u'de': u'Bundesamt für Meteorologie und Klimatologie MeteoSchweiz',
        u'fr': u'Office fédéral de météorologie et de climatologie MétéoSuisse',
        u'it': u'Ufficio federale di meteorologia e climatologia MeteoSvizzera',
        u'en': u'Federal Office of Meteorology and Climatology MeteoSwiss',
    }

    GROUPS = {
        u'de': [u'Raum und Umwelt'],
        u'fr': [u'Espace et environnement'],
        u'it': [u'Territorio e ambiente'],
        u'en': [u'Territory and environment']
    }

    def _get_s3_bucket(self):
        '''
        Create an S3 connection to the department bucket
        '''
        if not hasattr(self, '_bucket'):
            conn = S3Connection(self.AWS_ACCESS_KEY, self.AWS_SECRET_KEY)
            self._bucket = conn.get_bucket(self.BUCKET_NAME)
        return self._bucket

    def _fetch_metadata_file(self):
        '''
        Fetching the Excel metadata file from the S3 Bucket and save on disk
        '''
        try:
            metadata_file = Key(self._get_s3_bucket())
            metadata_file.key = self.METADATA_FILE_NAME
            metadata_file.get_contents_to_filename(self.METADATA_FILE_NAME)
        except Exception, e:
            log.exception(e)


    def _get_s3_resources(self, resources, s3_prefix):
        '''
        Lookup all files on S3, an match them with meta descriptions
        '''
        result = []

        for key in self._get_s3_bucket().list(s3_prefix):
            path = key.name.split('/')
            if len(path) >= 2 and path[0] == s3_prefix and key.size > 0:
                url = key.generate_url(0, query_auth=False, force_http=True)
                name = os.path.basename(key.name)

                data = {
                    u'url': url,
                    u'name': name,
                    u'format': self._guess_format(name),
                }

                description = self._description_lookup(resources, name)
                if description:
                    data.update({u'description': description})

                result.append(data)

        return result

    def _guess_format(self, path):
        return os.path.splitext(path.lower())[1][1:]

    def _description_lookup(self, resources, filename):
        '''
        Check if metafile declared a description to this resource
        '''
        basename, ext = os.path.splitext(filename)
        for resource in resources:
            if basename in resource.get('id', ''):
                return resource.get('description')
            if basename in resource.get('Standort', ''):
                return resource.get('description')


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

        ids = []
        for sheet_name, use_gm03_desc in self.SHEETS:
            log.debug('Gathering %s' % sheet_name)

            parser = MetaDataParser(self.METADATA_FILE_NAME)

            metadata = parser.parse_sheet(sheet_name, use_gm03_desc)
            metadata['translations'].extend(self._metadata_term_translations())
            metadata['sheet_name'] = sheet_name

            obj = HarvestObject(
                #guid = metadata.get('id'),
                job = harvest_job,
                content = json.dumps(metadata)
            )

            obj.save()
            ids.append(obj.id)

        return ids

    def fetch_stage(self, harvest_object):
        log.debug('In Meteoswiss fetch_stage')
        package_dict = json.loads(harvest_object.content)

        sheet_name = package_dict.get('sheet_name')
        s3_prefix = self.S3_PREFIXES.get(sheet_name)

        if s3_prefix:
            log.debug('Loading S3 Resources for %s' % sheet_name)
            package_dict['resources'] = self._get_s3_resources(
                package_dict.get('resources', []),
                s3_prefix
            )

            harvest_object.content = json.dumps(package_dict)
            harvest_object.save()

        return True

    def _create_uuid(self, name=None):
        '''
        Create a new SHA-1 uuid for a given name or a random id
        '''
        if name:
            new_uuid = uuid5(NAMESPACE_OID, str(name))
        else:
            new_uuid = uuid4()

        return unicode(new_uuid)

    def import_stage(self, harvest_object):
        log.debug('In Meteoswiss import_stage')

        if not harvest_object:
            log.error('No harvest object received')
            return False

        try:
            package_dict = json.loads(harvest_object.content)

            user = model.User.get(self.HARVEST_USER)

            context = {
                'model': model,
                'session': Session,
                'user': self.HARVEST_USER
            }

            package_dict['id'] = self._create_uuid(package_dict.get('id'))

            # Find or create group the dataset should get assigned to
            package_dict['groups'] = self._find_or_create_groups(context)

            # Find or create the organization the dataset should get assigned to
            package_dict['owner_org'] = self._find_or_create_organization(context)

            # Never import state from data source!
            if 'state' in package_dict:
                del package_dict['state']

            # Split tags
            tags = package_dict.get('tags', '').split(',')
            tags = [tag.strip() for tag in tags]

            if '' not in tags and '(tbd)' not in tags:
                package_dict['tags'] = tags
            else:
                del package_dict['tags']

            package = model.Package.get(package_dict['id'])
            model.PackageRole(package=package, user=user, role=model.Role.ADMIN)

            #log.debug('Save or update package %s' % (package_dict['name'],))
            result = self._create_or_update_package(package_dict, harvest_object)

            log.debug('Save or update term translations')
            self._submit_term_translations(context, package_dict)

            Session.commit()
        except Exception, e:
            log.exception(e)
            raise e
        return True

    def _find_or_create_groups(self, context):
        group_name = self.GROUPS['de'][0]
        data_dict = {
            'id': group_name,
            'name': self._gen_new_name(group_name),
            'title': group_name
            }
        try:
            group = get_action('group_show')(context, data_dict)
        except:
            group = get_action('group_create')(context, data_dict)
            log.info('created the group ' + group['id'])
        group_ids = []
        group_ids.append(group['id'])
        return group_ids

    def _find_or_create_organization(self, context):
        try:
            data_dict = {
                'permission': 'edit_group',
                'id': self._gen_new_name(self.ORGANIZATION[u'de']),
                'name': self._gen_new_name(self.ORGANIZATION[u'de']),
                'title': self.ORGANIZATION[u'de']
            }
            organization = get_action('organization_show')(context, data_dict)
        except:
            organization = get_action('organization_create')(context, data_dict)
        return organization['id']

    def _metadata_term_translations(self):
        '''
        Generate term translatations for organizations
        '''
        try:
            translations = []

            for lang, org in self.ORGANIZATION.items():
                if lang != u'de':
                    translations.append({
                        'lang_code': lang,
                        'term': self.ORGANIZATION[u'de'],
                        'term_translation': org
                    })

            return translations

        except Exception, e:
            log.exception(e)
            return []

    def _submit_term_translations(self, context, package_dict):
        for translation in package_dict['translations']:
            action.update.term_translation_update(context, translation)
