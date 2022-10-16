import os
import re
import datetime
from pathlib import Path

from dataclasses import dataclass
from typing import List, Any, Dict, Tuple, Callable


class shellcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    OFF = '\033[0m'


@dataclass
class BitrateCqPair:
    bitrate: int
    cq: int


_rxTimeParse = re.compile("[^;:]+")
_rxTimesParse = re.compile("([^-]+)(-([^-]+))?")
_rxTimesSplit = re.compile("[^ ]+")
_extensions: Dict[str, Callable[[int], BitrateCqPair]] = {}
_error: Callable[[str], None]


def init(extensions: Dict[str, Callable[[int], BitrateCqPair]], errorLog: Callable[[str], None]):
    global _extensions
    _extensions = extensions
    _error = errorLog


def defaultBitrateMod(bitrate: int) -> BitrateCqPair:
    if bitrate > 8000:
        return BitrateCqPair(8000, 26)
    elif bitrate > 5000:
        return BitrateCqPair(5000, 28)
    elif bitrate > 3000:
        return BitrateCqPair(3000, 30)
    else:
        return BitrateCqPair(None, 32)


def wmvBitrateMod(bitrate: int) -> BitrateCqPair:
    if bitrate > 8000:
        return BitrateCqPair(8000, 26)
    elif bitrate > 5000:
        return BitrateCqPair(5000, 28)
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

    def __init__(self, dirPath: Path, destPath: Path, fileName: str, name, times, videoLen, bitrate, ext, usrCq, mincq, maxcq):
        self.sourcePath = dirPath / fileName
        fext = self.sourcePath.suffix
        self.fileName = fileName
        self.name = name
        self.videoLen = videoLen
        self.timeStr = times
        self.sourceBitrate = bitrate
        self.times = []

        extMapping = _extensions[ext](bitrate)
        self.targetBitrate = extMapping.bitrate

        self.targetCq = extMapping.cq

        if usrCq:
            if isinstance(usrCq, str) and usrCq.startswith('mx'):
                usrCq = int(usrCq[2:])
                if self.targetCq > usrCq:
                    self.targetCq = usrCq
            elif isinstance(usrCq, str) and usrCq.startswith('m'):
                usrCq = int(usrCq[1:])
                if self.targetCq < usrCq:
                    self.targetCq = usrCq
            else:
                self.targetCq = int(usrCq)
        else:
            # print(f'name: {name[:24]}\tusrCq: {usrCq}\tmincq: {mincq} maxcq: {maxcq}\ttargetCq: {self.targetCq}')
            if mincq > maxcq:
                cqh = mincq
                mincq = maxcq
                maxcq = cqh

            if self.targetCq < mincq:
                self.targetCq = mincq
            elif self.targetCq > maxcq:
                self.targetCq = maxcq

        if not times == 'renc':
            for c in re.finditer(_rxTimesSplit, times):
                m = _rxTimesParse.search(c.group(0))

                if m and m.group(1):
                    self.times.append(TimeSpan(m.group(1), m.group(3), videoLen))

            self.multiTimes = len(self.times) > 1
        else:
            self.multiTimes = False

    def printTimes(self, pref, color = False):
        length = lambda t: re.sub('^0:', '', str(datetime.timedelta(seconds=t.length)))
        return f"\n{pref}".join(map(lambda t: f'{shellcolors.WARNING if color and t.length > 1800 else ""}{length(t)}\t{t.start} -> {t.end}{shellcolors.OFF}', self.times))


@dataclass
class EncodeBatch:
    files: List[EncodeConfig]
    destFolder: Path
    shortDir: str
