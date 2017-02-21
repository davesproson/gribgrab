import datetime
import re
import urllib
import urllib.parse
import requests

from itertools import chain

class IdxField(object):
    def __init__(self, idx_line):
        self.idx_data = idx_line.strip().split(':')

        self.index = int(self.idx_data[0])
        self.bytes_start = int(self.idx_data[1])
        self.reftime = datetime.datetime.strptime(
            self.idx_data[2].split('=')[1], '%Y%m%d%H'
        )
        self.varname = self.idx_data[3]
        self.level = self.idx_data[4]
        self.interval = self.idx_data[5]

    def __str__(self):
        return ':'.join([
            str(self.index), str(self.bytes_start),
            self.reftime.strftime('d=%Y%m%d%H'), self.varname,
            self.level, self.interval
        ])

    def __repr__(self):
        return '{!s}({!r})'.format(
            type(self).__name__,
            str(self)
        )


class IdxCollection(object):
    def __init__(self):
        self.i_to_idx = dict()
        self.idx_to_i = dict()

    def add_idx(self, idx):
        self.i_to_idx[idx.index] = idx
        self.idx_to_i[idx] = idx.index

    def byterange(self, idx):
        bytes_start = idx.bytes_start
        try:
            bytes_end = self.i_to_idx[idx.index+1].bytes_start - 1
        except KeyError:
            bytes_end = ''

        return (bytes_start, bytes_end)

    def filter(self, regex):
        matches = list()
        regex = re.compile(regex)

        for idx in self.i_to_idx.values():
            if regex.match(str(idx)) is not None:
                matches.append(idx)

        return matches





class NomadsDownloader(object):

    SERVER = 'www.ftp.ncep.noaa.gov'
    BASE_URL = '/data/nccf/com/gfs/prod/'
    STEPS = {
        0.25: chain(range(121), range(123, 241, 3), range(252, 385, 12)),
        0.5: chain(range(0, 241, 3), range(252, 385, 12)),
        1: chain(range(0, 241, 3), range(252, 385, 12)),
        2.5: chain(range(0, 241, 3), range(252, 385, 12))
    }
    VALID_RESOLUTIONS = [0.25, 0.5, 1, 2.5]
    FILE_PATTERN = 'gfs.t{cycle_hr:02d}z.pgrb2.{res_str}.f{step:03d}'

    def __init__(self, cycle, horizon=168, resolution=0.5, min_step=None):
        if resolution not in type(self).VALID_RESOLUTIONS:
            raise ValueError('resolution must be one of {}'.format(
                ','.join([str(i) for i in type(self).STEPS])
            ))

        self.regexes = []
        self.cycle = cycle
        self.horizon = horizon
        self.resolution = resolution
        self.min_step = min_step
        self.base_url = 'http://{}/{}/{}'.format(
            type(self).SERVER,
            type(self).BASE_URL,
            self.cycle.strftime('gfs.%Y%m%d%H/')
        )

        self.res_str = '{0:0.2f}'.format(self.resolution).replace('.', 'p')

        self.steps = list(type(self).STEPS[self.resolution])
        if self.min_step is not None:
            self.steps = [i for i in self.steps if not i % self.min_step]
        if self.horizon is not None:
            self.steps = [i for i in self.steps if i <= self.horizon]

        self.grib_files = [
            urllib.parse.urljoin(
                self.base_url,
                type(self).FILE_PATTERN.format(
                    cycle_hr=self.cycle.hour,
                    res_str=self.res_str,
                    step=i
                )
            ) for i in self.steps
        ]

        self.idx_files = [i + '.idx' for i in self.grib_files]

    def _idx_files_exist(self):
        for idx in self.idx_files:
            if requests.head(idx).status_code != 200:
                return False
        return True

    def add_regex(self, regex):
        self.regexes.append(regex)

    def add_regexes(self, regexes):
        for regex in regexes:
            self.add_regex(regex)

    def _get_idx_data(self, idx_file):
        c = IdxCollection()

        with urllib.request.urlopen(idx_file) as response:
            idx_lines = response.readlines()

        for i in idx_lines:
            c.add_idx(IdxField(i.decode('utf-8')))

        return c

    def download(self, filename='out.grb2'):
        for i, idx_file in enumerate(self.idx_files):
            byte_ranges = []
            idx_collection = self._get_idx_data(idx_file)
            for regex in self.regexes:
                idx_list = idx_collection.filter(regex)
                for idx in idx_list:
                    byte_ranges.append(idx_collection.byterange(idx))

            print('downloading {} with byteranges {}'.format(
                idx_file, ','.join([str(i) for i in byte_ranges])))

            byte_header = 'bytes={}'.format(
                ','.join([str(i[0]) + '-' + str(i[1])
                          for i in byte_ranges]))

            url_path = urllib.parse.urlparse(self.grib_files[i]).path
            conn = http.client.HTTPConnection(type(self).SERVER)
            conn.request('GET', url_path, headers={'Range': byte_header})
            resp = conn.getresponse()
            with open(filename, 'wb') as fout:
                fout.write(resp.read())
