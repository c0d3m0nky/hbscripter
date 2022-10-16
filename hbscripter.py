import sys
import os
import stat
import re
import datetime
import traceback
import math
from functools import reduce
from pathlib import Path

from typing import List, Any, Dict, Tuple

import cv2

import encodingCommon as enc

shellcolors = enc.shellcolors

_args = sys.argv.copy()
_plan = False
_planOnly = False
_clean = False
_singleQueue: List[enc.EncodeBatch] = None
_bash = False
_shell = False
_usebitrate = False
_destFolderName = '__..c'
_rootMap = None
_trace = False

if '--bash' in _args:
    _bash = True
    _args.remove('--bash')
elif not '--win' in _args and (sys.platform == "linux" or sys.platform == "linux2"):
    _shell = True

if '--plan' in _args:
    _planOnly = True
    _singleQueue = []
    _args.remove('--plan')

if '--clean' in _args:
    _clean = True
    _args.remove('--clean')

if '--usebitrate' in _args:
    _plan = True
    _args.remove('--usebitrate')

if '--sq' in _args:
    _singleQueue = []
    _args.remove('--sq')

if '-t' in _args:
    _trace = True
    _args.remove('-t')

_rootDir = Path('/mnt/user/ngt/_conv/')

_rxTimesStr = re.compile("^(.+)~([\d;:\- ]+)( \[([mqx]*?\d+)\])*$")
_rxRencStr = re.compile("^(.+)~(renc)( \[([mqx]*?\d+)\])*$")

_extensions = {
    '.mp4': enc.defaultBitrateMod,
    '.mov': enc.defaultBitrateMod,
    '.ts': enc.defaultBitrateMod,
    '.avi': enc.defaultBitrateMod,
    '.mkv': enc.defaultBitrateMod,
    '.wmv': enc.wmvBitrateMod,
    '.m4v': enc.defaultBitrateMod,
    '.mpg': enc.defaultBitrateMod,
    '.flv': enc.defaultBitrateMod
}


def logts() -> str:
    return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def error(msg) -> None:
    print(f'{shellcolors.FAIL}{logts()}\tERR\t{msg}{shellcolors.OFF}')


def log(msg, color='') -> None:
    print(f'{color or (shellcolors.OKBLUE if _trace else "")}{logts()}\tLOG\t{msg}{shellcolors.OFF}')


def logTrace(msg) -> None:
    if _trace:
        print(f'{logts()}\tTRC\t{msg}')


enc.init(_extensions, error)


def cmdPathMap(path: Path):
    qc = "'" if _bash else '"'
    if _shell:
        return qc + str(path).replace('"', '""') + qc
    else:
        return qc + str(path).replace('/', '\\').replace('\\mnt\\user\\', '\\\\rmofrequirement.local\\') + qc


def writeQueue(batches: List[enc.EncodeBatch], queueDir: Path):

    cmds = []
    tot = reduce(lambda a,b: a + len(b.files), batches, 0)
    log(f'Queue Size: {tot}')
    
    if _planOnly:
        return

    i = 0

    for b in batches:
        if not b.destFolder.exists() and b.files:
            os.makedirs(b.destFolder)

        for f in b.files:
            i += 1
            cmd = ''

            if _bash:
                cmd = f'./HandBrakeCLI.exe --preset "H.265 NVENC 1080p"'
            elif _shell:
                cmd = f'HandBrakeCLI --preset "H.265 NVENC 1080p"'
            else:
                cmd = f'title "{i}/{tot} {f.name}" && HandBrakeCLI.exe --preset "H.265 NVENC 1080p"'

            if _usebitrate:
                cmd += f' --vb {f.targetBitrate}'
                quality = ''
            else:
                cmd += f' -q {f.targetCq}.0'
                quality = f'-cq{f.targetCq}'

            if _rootMap:
                sourcePath = Path(f.sourcePath.as_posix().replace(_rootMap[0], _rootMap[1]))
            else:
                sourcePath = f.sourcePath

            cmd += f' -i {cmdPathMap(sourcePath)}'

            if _rootMap:
                destFolder = Path(b.destFolder.as_posix().replace(_rootMap[0], _rootMap[1]))
            else:
                destFolder = b.destFolder

            if f.times:
                ti = 0
                for t in f.times:
                    cnt = f'-{ti}' if f.multiTimes else ''
                    destPath = destFolder / f'{f.name}{cnt}-nvenc{quality}.mp4'
                    cmds.append(cmd + f' -o {cmdPathMap(destPath)} --start-at seconds:{t.start} --stop-at seconds:{t.length}')
                    ti += 1
            else:
                destPath = destFolder / f'{f.name}-nvenc{quality}.mp4'
                cmds.append(cmd + f'  -o {cmdPathMap(destPath)}')

            if _bash or _shell:
                cmds.append(f'mv {cmdPathMap(sourcePath)} {cmdPathMap(destFolder / f.fileName)}')
            else:
                cmds.append(f'move {cmdPathMap(sourcePath)} {cmdPathMap(destFolder / f.fileName)}')

    if cmds:
        if not (_bash or _shell):
            cmds.append(f'title "Queue Completed"')
        makesh = (_shell and _singleQueue)
        qfp = os.path.join(queueDir, 'queue.sh' if makesh else 'queue.txt')
        qf = open(qfp, "w")
        qf.truncate()
        qf.write((" &&\n" if _bash or _shell else " && ^\n").join(cmds))
        qf.close()
        st = os.stat(qfp)
        os.chmod(qfp, st.st_mode | stat.S_IEXEC)


def createBatch(encFiles: List[enc.EncodeConfig], destFolder: Path, shortDir: str, dir: Path, noopt: bool):
    pref = "\t - "
    fileStrs = "\n\t".join(map(lambda ef: f'{shellcolors.BOLD}{ef.name}{shellcolors.OFF}\t{shellcolors.OKBLUE}{ef.sourceBitrate} ~> {ef.targetCq}{shellcolors.OFF}\n{pref}{ef.printTimes(pref, True)}', encFiles))
    nooptMsg = f'    {shellcolors.WARNING}No options set' if noopt else ''
    print(f'{shellcolors.BOLD}Files in {shortDir}{nooptMsg}\n\t{fileStrs}{shellcolors.OFF}')

    if not _plan:
        batch = enc.EncodeBatch(encFiles, destFolder, shortDir)

        if _singleQueue is None:
            writeQueue([batch], dir)
        else:
            _singleQueue.append(batch)


def scanDir(dir: Path):
    shortDir = re.sub('^/', '', str(dir).replace(_rootDir.as_posix(), ""))
    logTrace(f'Checking {shortDir}')
    files = [f for f in dir.glob('*') if f.is_file() and f.suffix.lower() in _extensions.keys()]
    optfiles = dir.glob('_.*')
    cq = None
    mcq = 28
    mxcq = 50
    skipResCheck = False
    noopt = True

    if files:
        for f in optfiles:
            opt = f.name
            logTrace(f'optfile: {opt}')
            if '.src' in opt:
                noopt = False
                skipResCheck = True
                opt = opt.replace('.src', '')
            if opt.startswith('_.mcq.'):
                noopt = False
                mcq = int(opt[6:])
                logTrace(f'mcq: {mcq}')
            if opt.startswith('_.cq.'):
                noopt = False
                cqv = opt[5:]
                if '-' in cqv:
                    cqs = cqv.split('-')
                    mcq = int(cqs[0])
                    mxcq = int(cqs[1])
                    logTrace(f'mcq: {mcq}')
                    logTrace(f'mxcq: {mxcq}')
                else:
                    cq = int(cqv)
                    logTrace(f'cq: {cq}')

        destFolder = dir / _destFolderName

        encFiles = []
        fileStrs = "\n\t".join(map(lambda f: str(f), files))

        for f in files:
            if f.stem.startswith('~'):
                continue
            ms = _rxTimesStr.search(f.stem)
            rencMs = _rxRencStr.search(f.stem)

            if ms or rencMs:
                cleanPath = str(f).replace(_rootDir.as_posix(), '')
                try:
                    ext = f.suffix.lower()
                    v = cv2.VideoCapture(str(f))
                    fps = v.get(cv2.CAP_PROP_FPS)
                    frames = int(v.get(cv2.CAP_PROP_FRAME_COUNT))
                    vlen = math.ceil(frames / fps) + 1
                    vkb = (f.stat().st_size / 1000) * 8
                    bitrate = math.ceil(vkb / vlen)
                    if not skipResCheck:
                        height = v.get(cv2.CAP_PROP_FRAME_HEIGHT)
                        width = v.get(cv2.CAP_PROP_FRAME_WIDTH)
                        res = height if height < width else width

                        if res < 720 and mcq < 30:
                            mcq = 30
                            if mxcq < mcq:
                                mxcq = mcq
                        elif res < 1080 and mcq < 28:
                            mcq = 28
                            if mxcq < mcq:
                                mxcq = mcq

                    if rencMs:
                        name = rencMs.group(1)
                        times = 'renc'
                        encBitrate = rencMs.group(4)
                    else:
                        name = ms.group(1)
                        times = ms.group(2)
                        encBitrate = ms.group(4)
                    encFiles.append(enc.EncodeConfig(dir, destFolder, f.name, name, times, vlen, bitrate, ext, encBitrate or cq, mcq, mxcq))
                except Exception as e:
                    error(f'Error parsing {cleanPath}\n{e}')
                    traceback.print_exc()

        if encFiles:
            encFiles.sort(key=lambda x: x.name.casefold())
            createBatch(encFiles, destFolder, shortDir, dir, noopt)
    else:
        log(f'Folder is empty: {shortDir}', shellcolors.OKGREEN)


def defaultSorter(s: str):
    return s.casefold()


def nautilusSorter(s: str):
    return s.strip('/').strip('_').casefold()


def run():
    log(f'Scanning {_rootDir}')
    scanDirs = [_rootDir]
    cleanup = []

    for subdir, dirs, files in os.walk(_rootDir):
        for d in dirs:
            fdir = Path(os.path.join(subdir, d))
            if d == _destFolderName:
                if [f for f in fdir.glob('*') if f.is_file()]:
                    cleanup.append(str(fdir).replace(_rootDir.as_posix(), ''))
                continue
            elif d.startswith('.') or d.startswith('__..') or d.startswith('_.'):
                continue
            elif d == '___hbscripter' or '___hbscripter' in subdir:
                continue
            elif '__..' in subdir:
                continue
            scanDirs.append(fdir)
    sorter = nautilusSorter if _shell else defaultSorter

    if cleanup:
        cleanup.sort(key=lambda x: sorter(x))
        lst = '\n\t'.join(cleanup)
        log(f'Media to cleanup:\n\t{lst}\n', shellcolors.OKGREEN)
    
    if _clean:
        return

    for d in scanDirs:
        scanDir(d)

    if _singleQueue is not None:
        _singleQueue.sort(key=lambda x: sorter(x.shortDir))
        writeQueue(_singleQueue, Path(_rootDir))


if len(_args) > 1:
    _rootDir = Path(_args[1]).resolve()

if len(_args) > 3 and _args[2] == '--rm':
    _rootMap = _args[3].split(':')
    if not len(_rootMap) == 2:
        log(f'Bad root map: {_args[3]}')

if not os.path.isdir(_rootDir):
    log(f'Path does not exist {_rootDir}')
    sys.exit()

run()
