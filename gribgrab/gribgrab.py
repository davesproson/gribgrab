import datetime
import re
import http
import urllib
import urllib.parse
import requests

from itertools import chain

class DataNotAvailableError(Exception):
    pass

class IdxField(object):
    """
    An IdxField represents a single grib2 file index. A typical index looks
    like:
        8:900981:d=1995103000:UGRD:50 m above ground:165 hour fcst:
    """

    def __init__(self, idx_line):
        """
        Initialize an instance.

        args:
            idx_line - a string representing a singe grib2 message, as output
            by e.g. wgrib2 with no cmdline args.
        """

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
        ]) + ':'

    def __repr__(self):
        return '{!s}({!r})'.format(
            type(self).__name__,
            str(self)
        )


class IdxCollection(object):
    """
    An IdxCollection is a group of IdxFields. It therefore represents the
    contents of a grib2 index file.
    """

    def __init__(self):
        self.i_to_idx = {}
        self.idx_to_i = {}

    def __str__(self):
        return 'IdxCollection: {} fields'.format(
            len(self.i_to_idx)
        )


    def add_idx(self, idx):
        """
        Add an IdxField to this collection.

        args:
            idx - the IdxField to add
        """

        self.i_to_idx[idx.index] = idx
        self.idx_to_i[idx] = idx.index

    def byterange(self, idx):
        """
        Return the byterange associated with a particular grib2 message.

        args:
            idx - the IdxField whose byterange we're interested in.

        returns:
            a 2-tuple giving the start and end byte offsets of the assiciated
                grib2 message.
        """

        bytes_start = idx.bytes_start
        try:
            bytes_end = self.i_to_idx[idx.index+1].bytes_start - 1
        except KeyError:
            # We handle a KeyError as it will be raised when requesting the
            # last message in a file. However a KeyError may also be raised
            # if idx is not a member of this collection. TODO: handle this!
            bytes_end = ''

        return (bytes_start, bytes_end)

    def filter(self, regex):
        """
        Filter for IdxFields which match a given regex.

        args:
            regex - the regex to match

        returns:
            a list of IdxFields that match regex.
        """

        matches = []
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

    def _gribfile_to_step(self, gribfile):
        return self.steps[self.grib_files.index(gribfile)]

    def exists(self):
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

    def download(self, filename=None, file_template=None):
        """
        Download data. Either mergin into a single file (if filename is
        specified), or into individual files according to file_template.
        Only one of filename and file_template should be specified.

        kwargs:
            filename - a file to download all data to.
            file_template - a template to download individual files. May
                contain formatters for strfime, and *must* contain a 'step' key
                for str.format.
        """

        if not self.exists():
            raise DataNotAvailableError('index files do not exist on server')

        if filename is not None and file_template is not None:
            raise ValueError((
                'only one of \'filename\', \'file_template\' should be '
                'specified'
            ))

        for i, idx_file in enumerate(self.idx_files):

            if filename is None and file_template is None:
                # Neither filename or file_template are given, so maintain the
                # filename from the server
                _filename = os.path.basename(self.grib_files[i])
            elif file_template is None:
                # A single filename has been specified - use this. This will
                # result in all files from the server being merged into a
                # single local grib2 file.
                _filename = filename
            else:
                # A file pattern has been specified, so we're essentially
                # renaming the files as we download.
                _filename = self.cycle.strftime(file_template).format(
                    step=self._gribfile_to_step(self.grib_files[i])
                )

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
            with open(_filename, 'ab') as fout:
                fout.write(resp.read())

if __name__ == '__main__':
    cycle = datetime.datetime.utcnow().replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    n = NomadsDownloader(
        cycle,
        horizon=168,
        resolution=0.5,
    )

    n.add_regex('.*GRD:10 m above.*')
    n.add_regex('.*TMP:2 m above.*')

    n.download(file_template='gfs.t%Hz.{step:02d}.grb2')
