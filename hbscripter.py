import sys
import os
import pwd
import grp
import stat
import re
import datetime
import traceback
import math
from functools import reduce
from pathlib import Path

from typing import List, Any, Dict, Tuple, Callable

import cv2

import encodingCommon as enc

shellcolors = enc.shellcolors

_args = sys.argv.copy()
_max_fps = 31
_plan = False
_planOnly = False
_clean = False
_singleQueue: List[enc.EncodeBatch] = None
_shell = False
_usebitrate = False
_destFolderName = '__..c'
_rootMap = None
_trace = False
_nobody_uid = pwd.getpwnam("nobody").pw_uid
_users_gid = grp.getgrnam("users").gr_gid
_list_details = False
_list_fps = False
_list_fps_err = False
_list_fps_folder = False
_list_bitrate_limit = -1
_list_bitrate_error = False
_list_bitrate_folder = False
_min_bytes = -1
_file_filter = None
_dir_filter = None
_file_sorter = None

_file_filters = {
    'converted': r'(-nvenc-[cq\d]+)'
}

_dir_filters = {
    
}


def defaultSorter(f: Callable[Any, str], iter: List) -> List:
    return iter.sort(key=lambda x: enc.windows_file_sort_keys(f(x)))


def nautilusSorter(f: Callable[Any, str], iter: List) -> List:
    return iter.sort(key=lambda x: f(x).strip('/').strip('_').casefold())


if '--shell' in _args:
    _shell = True
    _file_sorter = nautilusSorter
    _args.remove('--shell')
elif '--win' in _args:
    _shell = False
    _file_sorter = defaultSorter
    _args.remove('--win')
else:    
    _shell = False
    _file_sorter = defaultSorter

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

if '--iff' in _args:
    enc.ignoreFpsFactor()
    _args.remove('--iff')

if '-t' in _args:
    _trace = True
    _args.remove('-t')

if '--fps' in _args:
    _list_details = True
    _list_fps = True
    _args.remove('--fps')

if '--fpse' in _args:
    _list_details = True
    _list_fps = True
    _list_fps_err = True
    _args.remove('--fpse')

if '--fpsf' in _args:
    _list_details = True
    _list_fps = True
    _list_fps_folder = True
    _args.remove('--fpsf')


def logts() -> str:
    return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def error(msg) -> None:
    print(f'{shellcolors.FAIL}{logts()}\tERR\t{msg}{shellcolors.OFF}')


def log(msg, color='') -> None:
    print(f'{color or (shellcolors.OKBLUE if _trace else "")}{logts()}\tLOG\t{msg}{shellcolors.OFF}')


def logTrace(msg) -> None:
    if _trace:
        print(f'{logts()}\tTRC\t{msg}')


_rootDir = Path('/mnt/user/ngt/_conv/')

if '--rd' in _args:
    i = _args.index('--rd') + 1
    _rootDir = Path(_args[i]).resolve()
    del _args[i]
    _args.remove('--rd')
else:
    if len(_args) > 1:
        _rootDir = Path(_args[1]).resolve()
        del _args[1]

if not os.path.isdir(_rootDir):
    log(f'Path does not exist {_rootDir}')
    sys.exit()

while len(_args) > 1:
    if _args[1] == '--rm':
        _rootMap = _args[2].split(':')
        
        if not len(_rootMap) == 2:
            log(f'Bad root map: {_args[2]}')
            sys.exit()

        del _args[2]
        _args.remove('--rm')
    elif _args[1] in ['--btr', '--btre', '--btrf']:
        _list_details = True
        
        if len(_args) > 2 and _args[2].isdigit():
            _list_bitrate_limit = int(_args[2])
            del _args[2]
        else:
            _list_bitrate_limit = 5

        if '--btre' in _args:
            _list_bitrate_error = True
            _args.remove('--btre')
        elif '--btrf' in _args:
            _list_bitrate_error = True
            _list_bitrate_folder = True
            _args.remove('--btrf')
        else:
            _args.remove('--btr')
    elif _args[1] == '--minmb':
        if len(_args) > 2 and _args[2].isdigit():
            _min_bytes = int(_args[2]) * 1000000
        else:
            log(f'Bad arg: {_args[1]} {_args[2]}')
            sys.exit()
        del _args[2]
        _args.remove('--minmb')
    elif _args[1] == '--filerx':
        if len(_args) > 2 and _args[2]:
            _file_filter = _args[2]
        else:
            log(f'Bad arg: {_args[1]} {_args[2]}')
            sys.exit()
        del _args[2]
        _args.remove('--filerx')

        if _file_filter in _file_filters:
            _file_filter = _file_filters[_file_filter]
    elif _args[1] == '--dirrx':
        if len(_args) > 2 and _args[2]:
            _dir_filter = _args[2]
        else:
            log(f'Bad arg: {_args[1]} {_args[2]}')
            sys.exit()
        del _args[2]
        _args.remove('--dirrx')

        if _dir_filter in _dir_filters:
            _dir_filter = _dir_filters[_dir_filter]
    else:
        log(f'Bad arg: {_args[1]}')
        sys.exit()

# if _list_details and _min_bytes == -1:
#     log('--minmb not set. Defaulting to 300mb')
#     _min_bytes = 300000000

_rxpOptions = '( \[(([mqxr]*?[0-9.]+ ?)*?)\])*'
_rxTimesStr = re.compile("^(.+)~([\d;:\- ]+)" + _rxpOptions + "$")
_rxRencStr = re.compile("^(.+)~(renc)" + _rxpOptions + "$")

_extensions = {
    '.mp4': enc.defaultBitrateMod,
    '.mov': enc.defaultBitrateMod,
    '.ts': enc.defaultBitrateMod,
    '.avi': enc.defaultBitrateMod,
    '.mkv': enc.defaultBitrateMod,
    '.wmv': enc.wmvBitrateMod,
    '.m4v': enc.defaultBitrateMod,
    '.mpg': enc.defaultBitrateMod,
    '.flv': enc.defaultBitrateMod,
    '.webm': enc.defaultBitrateMod,
    '.vid': enc.defaultBitrateMod
}


enc.init(_extensions, error)


def cmdPathMap(path: Path):
    qc = "'" if _shell else '"'
    if _shell:
        return qc + str(path).replace('"', '""') + qc
    else:
        return qc + str(path).replace('/', '\\').replace('\\mnt\\user\\', '\\\\rmofrequirement.local\\') + qc


def writeQueue(batches: List[enc.EncodeBatch], queueDir: Path):

    cmds = []
    #tot = reduce(lambda a,b: a + len(b.files), batches, 0)
    tot = reduce(lambda a,b: a + reduce(lambda at,bt: at + (1 if not bt.multiTimes else len(bt.times)), b.files, 0), batches, 0)
    log(f'Queue Size: {tot}')
    
    if _planOnly:
        return

    qi = 0

    for b in batches:
        if not b.destFolder.exists() and b.files:
            os.makedirs(b.destFolder)
            
        os.chown(b.destFolder, _nobody_uid, _users_gid)
        os.chmod(b.destFolder, 0o0777)

        for f in b.files:
            cmd = ''
            setTitle = False

            if _shell:
                cmd = f'HandBrakeCLI --preset "H.265 NVENC 1080p"'
            else:
                setTitle = True
                cmd = f'HandBrakeCLI.exe --preset "H.265 NVENC 1080p"'

            if _usebitrate:
                cmd += f' --vb {f.targetBitrate}'
                quality = ''
            else:
                cmd += f' -q {f.targetCq}.0'
                quality = f'-cq{f.targetCq}'

            if f.setfps:
                cmd += f' -r {f.setfps}'

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
                    qi += 1
                    cnt = f'-{ti}' if f.multiTimes else ''
                    destPath = destFolder / f'{f.name}{cnt}-nvenc{quality}.mp4'
                    title = f'title "{qi}/{tot} {f.name}" && ' if setTitle else ''
                    cmds.append(f'{title}{cmd} -o {cmdPathMap(destPath)} --start-at seconds:{t.start} --stop-at seconds:{t.length}')
                    ti += 1
            else:
                qi += 1
                destPath = destFolder / f'{f.name}-nvenc{quality}.mp4'
                title = f'title "{qi}/{tot} {f.name}" && ' if setTitle else ''
                cmds.append(f'{title}{cmd} -o {cmdPathMap(destPath)}')

            if _shell:
                cmds.append(f'mv {cmdPathMap(sourcePath)} {cmdPathMap(destFolder / f.fileName)}')
            else:
                cmds.append(f'move {cmdPathMap(sourcePath)} {cmdPathMap(destFolder / f.fileName)}')

    if cmds:
        if not _shell:
            cmds.append(f'title "Queue Completed"')
        makesh = (_shell and _singleQueue)
        qfp = os.path.join(queueDir, 'queue.sh' if makesh else 'queue.txt')
        qf = open(qfp, "w")
        qf.truncate()
        qf.write((" &&\n" if _shell else " && ^\n").join(cmds))
        qf.close()
        st = os.stat(qfp)
        os.chmod(qfp, st.st_mode | stat.S_IEXEC)


def flag_file(ef: enc.EncodeConfig):
    if ef.exclude:
        return shellcolors.FAIL + f'!{ef.excludeReason} {ef.name}'
    if ef.fps > 35 and not ef.setfps:
        ef.exclude = True
        return shellcolors.FAIL + f'!FPS:{ef.fps} {ef.name}'
    return ef.name


def createBatch(encFiles: List[enc.EncodeConfig], destFolder: Path, shortDir: str, dir: Path, noopt: bool):
    pref = "\t - "
    printTimes = lambda ef: '' if ef.isRenc else f'\n{pref}{ef.printTimes(pref, True)}'
    fileStrs = "\n\t".join(map(lambda ef: f'{shellcolors.BOLD}{flag_file(ef)}{ef.mods}{shellcolors.OFF}\t{shellcolors.WARNING if ef.resDropped else shellcolors.OKBLUE}{ef.sourceBitrate} ~> {ef.targetCq}{shellcolors.OFF}{printTimes(ef)}', encFiles))
    nooptMsg = f'    {shellcolors.WARNING}No options set' if noopt else ''
    print(f'{shellcolors.BOLD}Files in {shortDir if shortDir else "/"}{nooptMsg}\n\t{fileStrs}{shellcolors.OFF}')

    if not _plan:
        batch = list(filter(lambda ef: not ef.exclude, encFiles))
        if len(batch) > 0:
            batch = enc.EncodeBatch(batch, destFolder, shortDir)

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
            fmcq = mcq
            fmxcq = mxcq
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

                        if res < 720 and fmcq < 30:
                            fmcq = 30
                            if fmxcq < fmcq:
                                fmxcq = fmcq
                        elif res < 1080 and fmcq < 28:
                            fmcq = 28
                            if fmxcq < fmcq:
                                fmxcq = fmcq

                    if rencMs:
                        name = rencMs.group(1)
                        times = 'renc'
                        encBitrate = rencMs.group(4)
                    else:
                        name = ms.group(1)
                        times = ms.group(2)
                        encBitrate = ms.group(4)
                    ec = enc.EncodeConfig(dir, destFolder, f.name, name, times, vlen, fps, bitrate, ext, cq, encBitrate, mcq, mxcq)
                    if ec.targetCq > mxcq and not encBitrate:
                        ec.resDropped = True
                    encFiles.append(ec)
                except Exception as e:
                    error(f'Error parsing {cleanPath}\n{e}')
                    traceback.print_exc()

        if encFiles:
            _file_sorter(lambda x: x.name, encFiles)
            createBatch(encFiles, destFolder, shortDir, dir, noopt)
    else:
        log(f'Folder is empty: {shortDir}', shellcolors.OKGREEN)



def list_details(scanDirs):
    _file_sorter(lambda x: str(x), scanDirs)
    for dir in scanDirs:
        if _dir_filter and not re.search(_dir_filter, str(dir), flags=re.IGNORECASE):
            continue
        dir_clean = re.sub(r'^/', '', str(dir).replace(_rootDir.as_posix(), ''))
        dyn_file_count = 0
        files = [f for f in dir.glob('*') if f.is_file() and f.suffix.lower() in _extensions.keys()]
        _file_sorter(lambda x: x.name, files)

        for f in files:
            if _file_filter and not re.search(_file_filter, f.name, flags=re.IGNORECASE):
                continue
            if f.stat().st_size < _min_bytes:
                continue
            cleanPath = re.sub(r'^/','', str(f).replace(_rootDir.as_posix(), ''))
            v = cv2.VideoCapture(str(f))

            if _list_fps:
                fps = v.get(cv2.CAP_PROP_FPS)

                if _list_fps_folder:
                    if fps > _max_fps:
                        dyn_file_count += 1
                else:
                    if fps > _max_fps:
                        sc = shellcolors.FAIL
                    else:
                        sc = shellcolors.OKGREEN
                    
                    if not _list_fps_err or fps > _max_fps:
                        if _trace:
                            print(f'{sc}{cleanPath} : {fps}{shellcolors.OFF} {str(f)}')
                        else:
                            print(f'{sc}{cleanPath} : {fps}{shellcolors.OFF}')
            elif _list_bitrate_limit > 0:
                fps = v.get(cv2.CAP_PROP_FPS)
                frames = int(v.get(cv2.CAP_PROP_FRAME_COUNT))
                vlen = math.ceil(frames / fps) + 1
                vmb = (f.stat().st_size / 1000000) * 8
                bitrate = round(vmb / vlen, 1)

                if _list_bitrate_folder:
                    if bitrate > _list_bitrate_limit:
                        dyn_file_count += 1
                else:
                    mc = shellcolors.OKGREEN

                    if bitrate > _list_bitrate_limit:
                        mc = shellcolors.FAIL
                    elif _list_bitrate_error:
                        continue

                    if _trace:
                        print(f'{mc}{cleanPath}: {bitrate}{shellcolors.OFF} {str(dir)}')
                    else:
                        print(f'{mc}{cleanPath}: {bitrate}{shellcolors.OFF}')

        if dyn_file_count > 0:
            if _trace:
                print(f'{shellcolors.OKGREEN}{dir_clean}: {dyn_file_count}{shellcolors.OFF} {str(dir)}')
            else:
                print(f'{shellcolors.OKGREEN}{dir_clean}: {dyn_file_count}{shellcolors.OFF}')

    print('\n')


def scan_dirs(skip_dunder_dirs=True):
    log(f'Scanning {_rootDir}')
    scanDirs = [_rootDir]
    cleanup = []

    for subdir, dirs, files in os.walk(_rootDir):
        for d in dirs:
            fdir = Path(os.path.join(subdir, d))
            
            if skip_dunder_dirs and d == _destFolderName:
                if [f for f in fdir.glob('*') if f.is_file()]:
                    cleanup.append(str(fdir).replace(_rootDir.as_posix(), ''))
                continue
            elif skip_dunder_dirs and (d.startswith('.') or d.startswith('__..') or d.startswith('_.')):
                continue
            elif d == '___hbscripter' or '___hbscripter' in subdir:
                continue
            elif skip_dunder_dirs and '__..' in subdir:
                continue

            scanDirs.append(fdir)
    return (scanDirs, cleanup)



def run():
    if _list_details:
        (scanDirs, cleanup) = scan_dirs(skip_dunder_dirs=False)
        list_details(scanDirs)
    else:
        (scanDirs, cleanup) = scan_dirs()
        
        _file_sorter(lambda x: str(x), scanDirs)
        if cleanup:
            _file_sorter(lambda x: x, cleanup)
            lst = '\n\t'.join(cleanup)
            log(f'Media to cleanup:\n\t{lst}\n', shellcolors.OKGREEN)
        
        if _clean:
            return

        for d in scanDirs:
            scanDir(d)

        if _singleQueue is not None:
            print(len(_singleQueue))
            _file_sorter(lambda x: x.shortDir, _singleQueue)
            writeQueue(_singleQueue, Path(_rootDir))


run()
