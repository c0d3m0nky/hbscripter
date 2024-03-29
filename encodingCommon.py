import sys
import os
import re
import datetime
from pathlib import Path

from dataclasses import dataclass
from typing import List, Any, Dict, Tuple, Callable, Union

_ignoreFpsFactor = False


def ignore_fps_factor():
    global _ignoreFpsFactor

    _ignoreFpsFactor = True


def isint(s):
    try:
        int(s)
        return True
    except ValueError:
        return False


class shellcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    BLACK = '\033[30m'
    FAIL = '\033[91m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    OFF = '\033[0m'
    NOOP = ''


@dataclass
class BitrateCqPair:
    bitrate: int
    cq: int


_rxTimeParse = re.compile("[^;:]+")
_rxTimesParse = re.compile("([^-]+)(-([^-]+))?")
_rxTimesSplit = re.compile("[^ ]+")
_rxOptions = re.compile("([mqxr]+)([0-9.]+)")
_extensions: Dict[str, Callable[[int], BitrateCqPair]] = {}
_error: Callable[[str], None]


def init(extensions: Dict[str, Callable[[int], BitrateCqPair]], errorLog: Callable[[str], None]):
    global _extensions
    _extensions = extensions
    _error = errorLog


def defaultBitrateMod(bitrate: int) -> BitrateCqPair:
    if bitrate > 6000:
        return BitrateCqPair(6000, 26)
    elif bitrate > 3000:
        return BitrateCqPair(3000, 28)
    elif bitrate > 1000:
        return BitrateCqPair(1000, 30)
    else:
        return BitrateCqPair(None, 32)


def wmvBitrateMod(bitrate: int) -> BitrateCqPair:
    if bitrate > 6000:
        return BitrateCqPair(6000, 26)
    elif bitrate > 4000:
        return BitrateCqPair(4000, 28)
    elif bitrate > 3000:
        return BitrateCqPair(3000, 30)
    else:
        return BitrateCqPair(1500, 32)


def parseTime(time):
    seconds = 0
    part = 0

    for c in reversed(re.findall(_rxTimeParse, time)):
        try:
            v = int(c)

            if part == 0:
                seconds += v
            elif part == 1:
                seconds += v * 60
            elif part == 2:
                seconds += v * 60 * 60
            else:
                raise Exception('Time has too many parts')

            part += 1
        except Exception as e:
            _error(f'Failed parsing time: {c} {part}')
            raise

    return seconds


class TimeSpan:
    def __init__(self, start, end, videoLen):
        self.start = parseTime(start)
        self.end = parseTime(end) if end else videoLen

        if self.start >= self.end:
            raise Exception('Invalid start and end time')

        self.length = self.end - self.start


_valid_fps = [23.976, 24, 25, 29.97, 30, 48, 50, 59.94, 60, 72, 75, 90, 100, 120]


def is_invalid_fps(fps, orig_fps):
    global _ignoreFpsFactor

    if not isinstance(fps, float):
        return f'FPS_Type:{orig_fps}->{fps}'
    if fps == 0:
        return None
    if fps not in _valid_fps:
        return f'FPS_Value:{orig_fps}->{fps}'
    if fps > orig_fps == 0:
        return f'FPS_Greater:{orig_fps}->{fps}'

    mod = orig_fps % fps
    if mod >= 0.06 and fps - mod >= 0.06:
        if not _ignoreFpsFactor:
            return f'FPS_Factor:{orig_fps}->{fps} '
    return None


class EncodeConfig:
    sourcePath: Path
    fileName: str
    name: str
    timeStr: str
    videoLen: int
    sourceBitrate: int
    targetBitrate: int
    targetCq: int
    times: List[TimeSpan]
    multiTimes: bool
    resDropped: bool
    isRenc: bool
    fps: int
    setfps: float
    exclude: bool
    excludeReason: str
    mods: str

    def __init__(self, dirPath: Path, destPath: Path, fileName: str, name, times, videoLen, fps, bitrate, ext, parentcq, fileoptions, mincq, maxcq):
        self.sourcePath = dirPath / fileName
        fext = self.sourcePath.suffix
        self.fileName = fileName
        self.name = name
        self.videoLen = videoLen
        self.timeStr = times
        self.sourceBitrate = bitrate
        self.resDropped = False
        self.isRenc = True
        self.times = []
        self.fps = fps
        self.setfps = None
        self.exclude = False
        self.mods = ''

        extMapping = _extensions[ext](bitrate)
        self.targetBitrate = extMapping.bitrate

        self.targetCq = extMapping.cq
        optionsCq = None

        if fileoptions:
            if isinstance(fileoptions, int) or isint(fileoptions):
                optionsCq = int(fileoptions)
            else:
                for m in re.finditer(_rxOptions, fileoptions):
                    if m.group(1).startswith('mx'):
                        v = int(m.group(2))
                        if self.targetCq > v:
                            optionsCq = v
                    elif m.group(1).startswith('m'):
                        v = int(m.group(2))
                        if self.targetCq < v:
                            optionsCq = v
                    elif m.group(1).startswith('q'):
                        optionsCq = int(m.group(2))
                    elif m.group(1).startswith('r'):
                        f = float(m.group(2))
                        v = is_invalid_fps(f, fps)
                        if v:
                            self.exclude = True
                            self.excludeReason = v
                        else:
                            self.setfps = f
                            if not self.setfps == 0:
                                self.mods = f'\t{shellcolors.OKBLUE}FPS:{self.fps}->{self.setfps}'
        
        if optionsCq:
            self.targetCq = optionsCq
        elif parentcq and isinstance(parentcq, int):
            self.targetCq = parentcq
        else:
            # print(f'name: {name[:24]}\tfileoptions: {fileoptions}\tmincq: {mincq} maxcq: {maxcq}\ttargetCq: {self.targetCq}')
            if mincq > maxcq:
                cqh = mincq
                mincq = maxcq
                maxcq = cqh

            if self.targetCq < mincq:
                self.targetCq = mincq
            elif self.targetCq > maxcq:
                self.targetCq = maxcq

        if not times == 'renc':
            self.isRenc = False
            for c in re.finditer(_rxTimesSplit, times):
                m = _rxTimesParse.search(c.group(0))

                if m and m.group(1):
                    self.times.append(TimeSpan(m.group(1), m.group(3), videoLen))

            self.multiTimes = len(self.times) > 1
        else:
            self.multiTimes = False

    def printTimes(self, pref, color=False):
        length = lambda t: re.sub('^0:', '', str(datetime.timedelta(seconds=t.length)))
        return f"\n{pref}".join(map(lambda t: f'{shellcolors.WARNING if color and t.length > 1800 else ""}{length(t)}\t{t.start} -> {t.end}{shellcolors.OFF}', self.times))


@dataclass
class EncodeBatch:
    files: List[EncodeConfig]
    destFolder: Path
    shortDir: str


_rx_num_delim = re.compile(r'([^\d]|\d+)')
_max_neg = sys.maxsize * -1
_windows_sort_pos = {
    ' ': _max_neg + 1,
    '!': _max_neg + 2,
    '#': _max_neg + 3,
    '$': _max_neg + 4,
    '%': _max_neg + 5,
    '&': _max_neg + 6,
    '(': _max_neg + 7,
    ')': _max_neg + 8,
    ',': _max_neg + 9,
    '.': _max_neg + 10,
    '.': _max_neg + 11,
    '.': _max_neg + 12,
    ';': _max_neg + 13,
    '@': _max_neg + 14,
    '[': _max_neg + 15,
    ']': _max_neg + 16,
    '^': _max_neg + 17,
    '_': _max_neg + 18,
    '`': _max_neg + 19,
    '{': _max_neg + 20,
    '}': _max_neg + 21,
    '~': _max_neg + 22,
    '¡': _max_neg + 23,
    '´': _max_neg + 24,
    '¿': _max_neg + 25,
    '+': _max_neg + 26,
    '=': _max_neg + 27,
    '÷': _max_neg + 28,
    '·': _max_neg + 29,
    '\'': 1,
    '-': 2
}
_windows_sort_num_offset = _max_neg + 30


def windows_file_sort_keys(key: str) -> List:
    if not key:
        return [_max_neg]

    m = _rx_num_delim.findall(key.casefold())
    if m:
        i = 0

        while i < len(m):
            if m[i].isdigit():
                m[i] = _windows_sort_num_offset + int(m[i])
            else:
                v = _windows_sort_pos.get(m[i])

                if v == 0:
                    i += 1
                    continue
                elif v:
                    m[i] = v
                else:
                    m[i] = ord(m[i])
            i += 1
        return m
    else:
        return [key]


_dblcmd_sort_pos = {
    ' ': _max_neg + 1,
    '!': _max_neg + 2,
    '#': _max_neg + 3,
    '$': _max_neg + 4,
    '%': _max_neg + 5,
    '&': _max_neg + 6,
    '\'': _max_neg + 7,
    '(': _max_neg + 8,
    ')': _max_neg + 9,
    '+': _max_neg + 10,
    ',': _max_neg + 11,
    '-': _max_neg + 12,
    '.': _max_neg + 13,
    ';': _max_neg + 14,
    '=': _max_neg + 15,
    '@': _max_neg + 16,
    '[': _max_neg + 17,
    ']': _max_neg + 18,
    '^': _max_neg + 19,
    # '_': _max_neg + 20,
    '`': _max_neg + 21,
    '{': _max_neg + 22,
    '}': _max_neg + 23,
    '~': _max_neg + 24,
    '¡': _max_neg + 25,
    '´': _max_neg + 26,
    '·': _max_neg + 27,
    '¿': _max_neg + 28,
    '÷': _max_neg + 29
}
_dblcmd_sort_dunder_pre_alpha = ['-']
_dblcmd_sort_dunder_offset = _max_neg + 20
# leave buffer for dunder handling
_dblcmd_sort_pos_max = max(_dblcmd_sort_pos.values())
_dblcmd_sort_num_offset = ((_dblcmd_sort_pos_max - _max_neg) * 2) + _max_neg


def dblcmd_file_sort_keys(key: str, parent: Path) -> List:
    if not key:
        return [_max_neg]

    if parent is None:
        key_path = Path(key)
    else:
        key_path = Path(parent / key)

    if parent is None or key_path.is_file():
        key_suffix = key_path.suffix
        key_stem = re.sub(re.escape(key_suffix) + '$', '', key)
    else:
        key_suffix = ''
        key_stem = key

    res: List[Union[str, int]] = []

    m: List[str] = _rx_num_delim.findall(key_stem.casefold())
    if m:
        dblcmd_file_sort_key_parts(m, res)

        res.append(_max_neg)

        if key_suffix:
            dblcmd_file_sort_key_parts(_rx_num_delim.findall(key_suffix.casefold()), res)

        return res
    else:
        return [key]


def dblcmd_file_sort_key_parts(m: List[str], res: List[Union[str, int]]):
    i = 0

    while i < len(m):
        if m[i] == '_':
            undc = 0
            num = False
            follow = None
            i += 1

            while i < len(m):
                if m[i] == '_':
                    undc += 1
                elif m[i].isdigit():
                    i -= 1
                    num = True
                    break
                elif m[i] in _dblcmd_sort_dunder_pre_alpha:
                    follow = _dblcmd_sort_pos[m[i]] - _max_neg
                    i -= 1
                    break
                else:
                    i -= 1
                    break
                i += 1

            res.append(_dblcmd_sort_dunder_offset)
            if num:
                res.append(_dblcmd_sort_dunder_offset)
                res.append(undc)
                res.append(0)
            elif follow:
                res.append(_dblcmd_sort_dunder_offset)
                res.append(undc)
                res.append(follow)
            else:
                res.append(_dblcmd_sort_dunder_offset + 1)
                res.append(undc * -1)
                res.append(0)
        elif m[i].isdigit():
            res.append(_dblcmd_sort_num_offset + int(m[i]))
        else:
            v = _dblcmd_sort_pos.get(m[i])

            if v == 0:
                i += 1
                continue
            elif v:
                res.append(v)
            else:
                res.append(ord(m[i]))
        i += 1
