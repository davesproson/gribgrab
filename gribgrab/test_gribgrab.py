import datetime
import unittest

try:
    from gribgrab import gribgrab
except ImportError:
    import gribgrab

class ExistsTestCase(unittest.TestCase):

    def test_no_future_data(self):
        """Check exists() for data in the future returns False"""
        cycle = datetime.datetime.utcnow().now().replace(
            hour=0, minute=0, second=0, microsecond=0
        ) + datetime.timedelta(days=3)

        m = gribgrab.NomadsDownloader(
            cycle,
            resolution=0.5,
            horizon=168
        )

        self.assertFalse(m.exists())


class IdxFieldTestCase(unittest.TestCase):

    def setUp(self):
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

    def setUp(self):
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

