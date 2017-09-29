"""
Tests for gribgrab.
"""

import datetime
import glob
import os
import unittest

# Allow these tests to be run both from the commandline (within this dir), and
# using nosetests.
try:
    from gribgrab import gribgrab
except ImportError:
    import gribgrab

class ExistsTestCase(unittest.TestCase):
    """
    Tests to confirm that the presence or otherwise of grib data is correctly
    reported.
    """

    def test_no_future_data(self):
        """Check exists() for data in the future returns False"""
        cycle = datetime.datetime.utcnow().now().replace(
            hour=0, minute=0, second=0, microsecond=0
        ) + datetime.timedelta(days=3)

        downloader = gribgrab.NomadsDownloader(
            cycle,
            resolution=0.5,
            horizon=168
        )

        self.assertFalse(downloader.exists())

    def test_24hour_gfs_data_exist(self):
        """
        Check exists() for 24hr data is true. Note: may fail due to server
        issues.
        """
        cycle = datetime.datetime.utcnow().now().replace(
            hour=0, minute=0, second=0, microsecond=0
        ) - datetime.timedelta(days=1)

        downloader = gribgrab.NomadsDownloader(
            cycle,
            resolution=0.5,
            horizon=24
        )

        self.assertTrue(downloader.exists())

class DownloadTestCase(unittest.TestCase):
    """
    Check that downloading some files works.
    """

    def setUp(self):
        self.cycle = datetime.datetime.utcnow().now().replace(
            hour=0, minute=0, second=0, microsecond=0
        ) - datetime.timedelta(days=1)

        self.downloader = gribgrab.GFSDownloader(
            cycle=self.cycle,
            resolution=0.5,
            horizon=12
        )

        self.downloader.add_regex('.*GRD:10 m above.*')

    def test_24hour_gfs_data_download_serial(self):
        """
        Test we can download 1 day old 00z gfs data in serial. Note: may
        fail due to remote issues.
        """
        self.downloader.download(file_template='gfs.t%Hz.{step:02d}.grb2')

        self.assertTrue(os.path.isfile('gfs.t00z.00.grb2'))
        self.assertTrue(os.path.isfile('gfs.t00z.03.grb2'))
        self.assertTrue(os.path.isfile('gfs.t00z.06.grb2'))
        self.assertTrue(os.path.isfile('gfs.t00z.09.grb2'))
        self.assertTrue(os.path.isfile('gfs.t00z.12.grb2'))

        for grib2 in glob.glob('gfs*.grb2'):
            os.remove(grib2)

    def test_24hour_gfs_data_download_concurrent(self):
        """
        Test we can download 1 day old 00z gfs data in parallel. Note: may
        fail due to remote issues.
        """
        self.downloader.download(file_template='gfs.t%Hz.{step:02d}.grb2',
                                 concurrent=3)

        self.assertTrue(os.path.isfile('gfs.t00z.00.grb2'))
        self.assertTrue(os.path.isfile('gfs.t00z.03.grb2'))
        self.assertTrue(os.path.isfile('gfs.t00z.06.grb2'))
        self.assertTrue(os.path.isfile('gfs.t00z.09.grb2'))
        self.assertTrue(os.path.isfile('gfs.t00z.12.grb2'))

        for grib2 in glob.glob('gfs*.grb2'):
            os.remove(grib2)


class IdxFieldTestCase(unittest.TestCase):
    """
    Unit tests associated with gribgrab.IdxField.
    """

    def setUp(self):
        """Create a test IdxField"""
        self.idx_field = '6:637816:d=1995103000:UGRD:30 m above ground:165 hour fcst:'
        self.idx = gribgrab.IdxField(self.idx_field)

    def test_idx_index(self):
        """Check the idx index number is correctly parsed"""
        self.assertEqual(self.idx.index, 6)

    def test_idx_bytes_start(self):
        """Check the idx byte start is correctly parsed"""
        self.assertEqual(self.idx.bytes_start, 637816)

    def test_idx_reftime(self):
        """Check the idx reference time is correctly parsed"""
        self.assertEqual(self.idx.reftime, datetime.datetime(1995, 10, 30))

    def test_idx_varname(self):
        """Check the idx variable name is correctly parsed"""
        self.assertEqual(self.idx.varname, 'UGRD')

    def test_idx_level(self):
        """Check the idx level is correctly parsed"""
        self.assertEqual(self.idx.level, '30 m above ground')

    def test_idx_interval(self):
        """Check the idx lead time/interval is correctly parsed"""
        self.assertEqual(self.idx.interval, '165 hour fcst')

    def test_idx__str__(self):
        """Check the idx __str__ method is correct"""
        self.assertEqual(str(self.idx), self.idx_field)


class IdxCollectionTestCase(unittest.TestCase):
    """
    Unit tests associated with gribgrab.IdxCollection.
    """

    def setUp(self):
        """
        Setup temporary data for unit tests.
        """
        self.idx_fields = [
            '1:0:d=1995103000:GUST:surface:165 hour fcst:',
            '2:73573:d=1995103000:MSLET:mean sea level:165 hour fcst:',
            '3:263118:d=1995103000:PRES:surface:165 hour fcst:'
        ]

        self.idxes = [gribgrab.IdxField(i) for i in self.idx_fields]

        self.idx_collection = gribgrab.IdxCollection()

        for idx in self.idxes:
            self.idx_collection.add_idx(idx)

    def test_byterange_field_1(self):
        """Check the byterange for field 1"""
        idx_field = self.idxes[0]
        byterange = self.idx_collection.byterange(idx_field)
        self.assertEqual(byterange, (0, 73572))

    def test_byterange_field_2(self):
        """Check the byterange for field 2"""
        idx_field = self.idxes[1]
        byterange = self.idx_collection.byterange(idx_field)
        self.assertEqual(byterange, (73573, 263117))

    def test_byterange_field_3(self):
        """Check the byterange for field 3"""
        idx_field = self.idxes[2]
        byterange = self.idx_collection.byterange(idx_field)
        self.assertEqual(byterange, (263118, ''))

    def test_filter_pres(self):
        """Check filtering just for the PRES variable"""
        idxes = self.idx_collection.filter(regex='.*PRES.*')
        self.assertEqual(len(idxes), 1)
        self.assertEqual(str(idxes[0]), self.idx_fields[2])

    def test_filter_pres_or_gust(self):
        """Check filtering for PRES or GUST variables"""
        idxes = self.idx_collection.filter(regex='.*(PRES|GUST)+:.*')
        self.assertEqual(len(idxes), 2)

        self.assertEqual(str(idxes[0]), self.idx_fields[0])
        self.assertEqual(str(idxes[1]), self.idx_fields[2])


if __name__ == '__main__':
    unittest.main(verbosity=2)
