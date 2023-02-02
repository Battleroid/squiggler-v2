from squiggler import Excluder
import logging
import unittest

logging.getLogger('squiggler').setLevel(logging.INFO)


class TestExcluder(unittest.TestCase):
    def test_none(self):
        ex = Excluder('none')
        self.assertFalse(ex('nothing'))
        self.assertFalse(ex('some-index'))

    def test_prefix(self):
        ex = Excluder('prefix', ['backfill-', 'legal-'])
        self.assertTrue(ex('backfill-some-index'))
        self.assertTrue(ex('legal-index'))
        self.assertFalse(ex('not-a-match'))
        self.assertFalse(ex('not-a-backfill-legal'))

    def test_suffix(self):
        ex = Excluder('suffix', ['-backfill', '-legal'])
        self.assertFalse(ex('backfill-some-index'))
        self.assertFalse(ex('legal-index'))
        self.assertTrue(ex('security-backfill-legal'))
        self.assertTrue(ex('mysql-backfill'))

    def test_regex(self):
        ex = Excluder('regex', ['.*\-backfill\-.*', '.*\-abc\-.*\-foobar$'])
        self.assertFalse(ex('backfill-some-index'))
        self.assertFalse(ex('legal-abc-index'))
        self.assertTrue(ex('mysql-backfill-2018'))
        self.assertTrue(ex('some-index-abc-notimportant-foobar'))
