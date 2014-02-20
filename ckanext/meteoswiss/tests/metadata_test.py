# encoding: utf-8

import os

from unittest import TestCase

from ckanext.meteoswiss.helpers.metadata import MetaDataParser


class MetaDataParsingTest(TestCase):

    TEST_DIR_PATH = os.path.dirname(os.path.relpath(__file__))
    TEST_FILE = 'OGD@Bund_Metadaten_MeteoSchweiz.xlsx'
    TEST_FILE_PATH = os.path.join(TEST_DIR_PATH, TEST_FILE)

    def setUp(self):
        self.parser = MetaDataParser(self.TEST_FILE_PATH)

    def test_smn(self):
        data = self.parser.parse_sheet(u'SMN')

        self.assertEqual(data.get('id'), 'VQHA69')
        self.assertEqual(data.get('title'), 'Messdaten SMN (SwissMetNet)')
        self.assertEqual(data.get('maintainer_email'), 'kud@meteoschweiz.ch')

        resources = data.get('resources')
        self.assertEqual(len(resources), 2)
        for key in ('id', 'url', 'name', 'description'):
            self.assertIn(key, resources[0])
        self.assertEqual(resources[0].get('name'), 'messdaten_smn_legende')
        self.assertEqual(resources[1].get('name'), 'Messdaten SMN')

        translations = data.get('translations')
        self.assertGreater(len(translations), 0)
        for translation in translations:
            for key in ('lang_code', 'term', 'term_translation'):
                self.assertIn(key, translation)

    def test_smn3(self):
        data = self.parser.parse_sheet(u'SMN 3')

        self.assertEqual(data.get('id'), 'VQHA70')
        self.assertEqual(data.get('title'), 'Messdaten SMN3 (SwissMetNet)')

        self.assertEqual(len(data.get('resources')), 2)
        self.assertGreater(len(data.get('translations')), 0)

    def test_foehnindex(self):
        data = self.parser.parse_sheet(u'Föhnindex')

        self.assertEqual(data.get('id'), 'VQHA71')
        self.assertEqual(data.get('title'), 'Foehnindex')

        self.assertEqual(len(data.get('resources')), 2)
        self.assertGreater(len(data.get('translations')), 0)

    def test_homogene(self):
        data = self.parser.parse_sheet(u'HomogeneDaten')

        self.assertEqual(data.get('id'), 'VQAA60')
        self.assertEqual(data.get('title'), 'Homogene Monatsdaten')

        self.assertEqual(len(data.get('resources')), 2)
        self.assertGreater(len(data.get('translations')), 0)

    def test_klimanormwerte(self):
        data = self.parser.parse_sheet(u'Klimanormwerte', use_gm03_desc=True)

        self.assertEqual(data.get('id'), 'klimanormwerte')
        self.assertEqual(data.get('title'), 'Klimanormwerte')

        self.assertEqual(len(data.get('resources')), 34)
        for resource in data.get('resources'):
            self.assertNotEqual(resource.get('description'), '')

        self.assertGreater(len(data.get('translations')), 0)

    def test_webcams(self):
        data = self.parser.parse_sheet(u'Kamerabild', use_gm03_desc=True)

        self.assertEqual(data.get('id'), 'Webcams')
        self.assertEqual(data.get('title'), 'Webcams')

        self.assertEqual(len(data.get('resources')), 42)
        for resource in data.get('resources'):
            self.assertNotEqual(resource.get('description'), '')

        self.assertEqual(len(data.get('translations')), 0)
