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
from argparse import ArgumentParser

from typing import List, Any, Dict, Tuple, Callable

import encodingCommon as enc

shellcolors = enc.shellcolors

_file_filters = {
    'converted': r'(-nvenc-[cq\d]+)'
}

_dir_filters = {

}

ap = ArgumentParser()
ap.add_argument("-t", "--trace", action='store_true', help="Ignore FPs Factor error")
ap.add_argument("-rd", "--root-dir", type=str, help="Root directory")
ap.add_argument("-rm", "--root-map", type=str, help="Replace root path segment ([root segment]:[replacement segment])")
ap.add_argument("-tbr", "--target-bitrate", type=str, help="Target bitrate")
ap.add_argument("--win", action='store_true', help="Write queue for windows")
ap.add_argument("--plan", action='store_true', help="Don't write queue")
ap.add_argument("--clean", action='store_true', help="Show folders needing cleaning")
ap.add_argument("-iff", "--ignore-fps-factor", action='store_true', help="Ignore FPs Factor error")
ap.add_argument("-renc", "--renc", action='store_true', help="Reencode all")
ap.add_argument("-fps", "--list-fps", action='store_true', help="List FPS details")
ap.add_argument("-fpse", "--list-fps-error", action='store_true', help="List out of bounds FPS")
ap.add_argument("-fpsf", "--list-fps-folder", action='store_true', help="List FPS folder details")
ap.add_argument("-btrl", "--bitrate-limit", type=int, default=5, help="Override bitrate upper bound")
ap.add_argument("-btr", "--list-bitrate", action='store_true', help="List bitrate details")
ap.add_argument("-btre", "--list-bitrate-error", action='store_true', help="List out of bounds bitrates")
ap.add_argument("-btrf", "--list-bitrate-folder", action='store_true', help="List bitrate folder details")
ap.add_argument("-minmb", "--min-bytes", type=int, default=-1, help="Min file size in MB")
ap.add_argument("-ff", "--file-filter", type=str, choices=_file_filters.keys(), help="File filter")
ap.add_argument("-df", "--dir-filter", type=str, choices=_dir_filters.keys(), help="Directory filter")
_args = ap.parse_args()

# takes a while, so avoid if --help called
import cv2


def default_sorter(f: Callable[[Any], str], iterr: List):
    iterr.sort(key=lambda x: enc.windows_file_sort_keys(f(x)))


def nautilus_sorter(f: Callable[[Any], str], iterr: List):
    iterr.sort(key=lambda x: f(x).strip('/').strip('_').casefold())


_max_fps = 31
_single_queue = []
_dest_folder_name = '__..c'
_nobody_uid = pwd.getpwnam("nobody").pw_uid
_users_gid = grp.getgrnam("users").gr_gid

_rxpOptions = r'( \[(([mqxr]*?[0-9.]+ ?)*?)\])*'
_rxTimesStr = re.compile(r'^(.+)~([\d;:\- ]+)' + _rxpOptions + "$")
_rxRencStr = re.compile('^(.+)~(renc)' + _rxpOptions + "$")

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

_root_map = None
_list_details = _args.list_fps or _args.list_fps_error or _args.list_fps_folder or _args.list_bitrate or _args.list_bitrate_error or _args.list_bitrate_folder
_list_fps = _args.list_fps or _args.list_fps_error or _args.list_fps_folder
_list_bitrate = _args.list_bitrate or _args.list_bitrate_error or _args.list_bitrate_folder
_file_filter = None
_dir_filter = None
_file_sorter = default_sorter if _args.win else nautilus_sorter
_root_dir = Path(_args.root_dir).resolve() if _args.root_dir else Path('./').resolve()

if _args.ignore_fps_factor:
    enc.ignore_fps_factor()

if not os.path.isdir(_root_dir):
    print(f'Path does not exist {_root_dir}')
    sys.exit()

if _args.root_map:
    _root_map = _args.root_map.split(':')

    if not len(_root_map) == 2:
        print(f'Bad root map: {_args.root_map}')
        sys.exit()

if _args.file_filter:
    _file_filter = _file_filters[_args.file_filter]

if _args.dir_filter:
    _dir_filter = _dir_filters[_args.dir_filter]


def logts() -> str:
    return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def error(msg) -> None:
    print(f'{shellcolors.FAIL}{logts()}\tERR\t{msg}{shellcolors.OFF}')


def log(msg, color='') -> None:
    print(f'{color or (shellcolors.OKBLUE if _args.trace else "")}{logts()}\tLOG\t{msg}{shellcolors.OFF}')


def log_trace(msg) -> None:
    if _args.trace:
        print(f'{logts()}\tTRC\t{msg}')


enc.init(_extensions, error)


def cmd_path_map(path: Path):
    qc = '"'
    if _args.win:
        return qc + str(path).replace('/', '\\').replace('\\mnt\\user\\', '\\\\rmofrequirement.local\\') + qc
    else:
        return qc + str(path).replace('"', '""') + qc


def write_queue(batches: List[enc.EncodeBatch], queue_dir: Path) -> object:
    cmds = []
    # tot = reduce(lambda a,b: a + len(b.files), batches, 0)
    tot = reduce(lambda a, b: a + reduce(lambda at, bt: at + (1 if not bt.multiTimes else len(bt.times)), b.files, 0), batches, 0)
    log(f'Queue Size: {tot}')

    if _args.plan:
        return

    qi = 0

    for b in batches:
        if not b.destFolder.exists() and b.files:
            os.makedirs(b.destFolder)

        os.chown(b.destFolder, _nobody_uid, _users_gid)
        os.chmod(b.destFolder, 0o0777)

        for f in b.files:
            cmd = ''
            set_title = False

            if _args.win:
                set_title = True
                cmd = f'HandBrakeCLI.exe --preset "H.265 NVENC 1080p"'
            else:
                cmd = f'HandBrakeCLI --preset "H.265 NVENC 1080p"'

            if _args.target_bitrate:
                cmd += f' --vb {f.targetBitrate}'
                quality = ''
            else:
                cmd += f' -q {f.targetCq}.0'
                quality = f'-cq{f.targetCq}'

            if f.setfps:
                cmd += f' -r {f.setfps}'

            if _root_map:
                source_path = Path(f.sourcePath.as_posix().replace(_root_map[0], _root_map[1]))
                dest_folder = Path(b.destFolder.as_posix().replace(_root_map[0], _root_map[1]))
            else:
                source_path = f.sourcePath
                dest_folder = b.destFolder

            cmd += f' -i {cmd_path_map(source_path)}'

            if f.times:
                ti = 0
                for t in f.times:
                    qi += 1
                    cnt = f'-{ti}' if f.multiTimes else ''
                    dest_path = dest_folder / f'{f.name}{cnt}-nvenc{quality}.mp4'
                    title = f'title "{qi}/{tot} {f.name}" && ' if set_title else ''
                    cmds.append(f'{title}{cmd} -o {cmd_path_map(dest_path)} --start-at seconds:{t.start} --stop-at seconds:{t.length}')
                    ti += 1
            else:
                qi += 1
                dest_path = dest_folder / f'{f.name}-nvenc{quality}.mp4'
                title = f'title "{qi}/{tot} {f.name}" && ' if set_title else ''
                cmds.append(f'{title}{cmd} -o {cmd_path_map(dest_path)}')

            if _args.win:
                cmds.append(f'move {cmd_path_map(source_path)} {cmd_path_map(dest_folder / f.fileName)}')
            else:
                cmds.append(f'mv {cmd_path_map(source_path)} {cmd_path_map(dest_folder / f.fileName)}')

    if cmds:
        if _args.win:
            cmds.append(f'title "Queue Completed"')
        qfp = os.path.join(queue_dir, 'queue.txt' if _args.win else 'queue.sh')
        qf = open(qfp, "w")
        qf.truncate()
        qf.write((" && ^\n" if _args.win else " &&\n").join(cmds))
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


def create_batch(enc_files: List[enc.EncodeConfig], dest_folder: Path, short_dir: str, full_dir: Path, noopt: bool):
    pref = "\t - "

    def print_times(ef):
        return '' if ef.isRenc else f'\n{pref}{ef.printTimes(pref, True)}'

    file_strs = "\n\t".join(map(
        lambda
            ef: f'{shellcolors.BOLD}{flag_file(ef)}{ef.mods}{shellcolors.OFF}\t{shellcolors.WARNING if ef.resDropped else shellcolors.OKBLUE}{ef.sourceBitrate} ~> {ef.targetCq}{shellcolors.OFF}{print_times(ef)}',
        enc_files
    ))
    no_opt_msg_color = shellcolors.WARNING if noopt else ''
    no_opt_msg = f'\tNo options set' if noopt else ''
    print(f'{shellcolors.BOLD}{no_opt_msg_color}Files in {short_dir if short_dir else "/"}{no_opt_msg}{shellcolors.OFF}\n\t{file_strs}{shellcolors.OFF}')

    if not _args.plan:
        batch = list(filter(lambda ef: not ef.exclude, enc_files))
        if len(batch) > 0:
            batch = enc.EncodeBatch(batch, dest_folder, short_dir)
            _single_queue.append(batch)


def scan_dir(full_dir: Path):
    short_dir = re.sub('^/', '', str(full_dir).replace(_root_dir.as_posix(), ""))
    log_trace(f'Checking {short_dir}')
    files = [f for f in full_dir.glob('*') if f.is_file() and f.suffix.lower() in _extensions.keys()]
    optfiles = full_dir.glob('_.*')
    cq = None
    mcq = 28
    mxcq = 50
    skip_res_check = False
    noopt = True

    if files:
        for f in optfiles:
            opt = f.name
            log_trace(f'optfile: {opt}')
            if '.src' in opt:
                noopt = False
                skip_res_check = True
                opt = opt.replace('.src', '')
            if opt.startswith('_.mcq.'):
                noopt = False
                mcq = int(opt[6:])
                log_trace(f'mcq: {mcq}')
            if opt.startswith('_.cq.'):
                noopt = False
                cqv = opt[5:]
                if '-' in cqv:
                    cqs = cqv.split('-')
                    mcq = int(cqs[0])
                    mxcq = int(cqs[1])
                    log_trace(f'mcq: {mcq}')
                    log_trace(f'mxcq: {mxcq}')
                else:
                    cq = int(cqv)
                    log_trace(f'cq: {cq}')

        dest_folder = full_dir / _dest_folder_name

        enc_files = []

        for f in files:
            fmcq = mcq
            fmxcq = mxcq
            if f.stem.startswith('~') or f.stem.startswith('!!'):
                continue
            ms = _rxTimesStr.search(f.stem)
            renc_ms = _rxRencStr.search(f.stem)

            if ms or renc_ms or _args.renc:
                clean_path = str(f).replace(_root_dir.as_posix(), '')
                try:
                    ext = f.suffix.lower()
                    v = cv2.VideoCapture(str(f))
                    fps = v.get(cv2.CAP_PROP_FPS)
                    frames = int(v.get(cv2.CAP_PROP_FRAME_COUNT))
                    vlen = math.ceil(frames / fps) + 1
                    vkb = (f.stat().st_size / 1000) * 8
                    bitrate = math.ceil(vkb / vlen)
                    if not skip_res_check:
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

                    if renc_ms:
                        name = renc_ms.group(1)
                        times = 'renc'
                        enc_bitrate = renc_ms.group(4)
                    elif not ms and _args.renc:
                        name = f.stem
                        times = 'renc'
                        enc_bitrate = ''
                    else:
                        name = ms.group(1)
                        times = ms.group(2)
                        enc_bitrate = ms.group(4)
                    ec = enc.EncodeConfig(full_dir, dest_folder, f.name, name, times, vlen, fps, bitrate, ext, cq, enc_bitrate, mcq, mxcq)
                    if ec.targetCq > mxcq and not enc_bitrate:
                        ec.resDropped = True
                    enc_files.append(ec)
                except Exception as e:
                    error(f'Error parsing {clean_path}\n{e}')
                    traceback.print_exc()

        if enc_files:
            _file_sorter(lambda x: x.name, enc_files)
            create_batch(enc_files, dest_folder, short_dir, full_dir, noopt)
    else:
        log(f'Folder is empty: {short_dir}', shellcolors.OKGREEN)


def list_details(dirs):
    _file_sorter(lambda x: str(x), dirs)
    for d in dirs:
        if _dir_filter and not re.search(_dir_filter, str(d), flags=re.IGNORECASE):
            continue
        dir_clean = re.sub(r'^/', '', str(d).replace(_root_dir.as_posix(), ''))
        dyn_file_count = 0
        files = [f for f in d.glob('*') if f.is_file() and f.suffix.lower() in _extensions.keys()]
        _file_sorter(lambda x: x.name, files)

        for f in files:
            if _file_filter and not re.search(_file_filter, f.name, flags=re.IGNORECASE):
                continue
            if f.stat().st_size < _args.min_bytes:
                continue
            clean_path = re.sub(r'^/', '', str(f).replace(_root_dir.as_posix(), ''))
            v = cv2.VideoCapture(str(f))

            if _list_fps:
                fps = v.get(cv2.CAP_PROP_FPS)

                if _args.list_bitrate_folder:
                    if fps > _max_fps:
                        dyn_file_count += 1
                else:
                    if fps > _max_fps:
                        sc = shellcolors.FAIL
                    else:
                        sc = shellcolors.OKGREEN

                    if not _args.list_bitrate_error or fps > _max_fps:
                        if _args.trace:
                            print(f'{sc}{clean_path} : {fps}{shellcolors.OFF} {str(f)}')
                        else:
                            print(f'{sc}{clean_path} : {fps}{shellcolors.OFF}')
            elif _list_bitrate > 0:
                fps = v.get(cv2.CAP_PROP_FPS)
                frames = int(v.get(cv2.CAP_PROP_FRAME_COUNT))
                vlen = math.ceil(frames / fps) + 1
                vmb = (f.stat().st_size / 1000000) * 8
                bitrate = round(vmb / vlen, 1)

                if _args.list_bitrate_folder:
                    if bitrate > _args.bitrate_limit:
                        dyn_file_count += 1
                else:
                    mc = shellcolors.OKGREEN

                    if bitrate > _args.bitrate_limit:
                        mc = shellcolors.FAIL
                    elif _args.list_bitrate_error:
                        continue

                    if _args.trace:
                        print(f'{mc}{clean_path}: {bitrate}{shellcolors.OFF} {str(d)}')
                    else:
                        print(f'{mc}{clean_path}: {bitrate}{shellcolors.OFF}')

        if dyn_file_count > 0:
            if _args.trace:
                print(f'{shellcolors.OKGREEN}{dir_clean}: {dyn_file_count}{shellcolors.OFF} {str(d)}')
            else:
                print(f'{shellcolors.OKGREEN}{dir_clean}: {dyn_file_count}{shellcolors.OFF}')

    print('\n')


def scan_dirs(skip_dunder_dirs=True):
    log(f'Scanning {_root_dir}')
    sdirs = [_root_dir]
    cleanup = []

    for subdir, dirs, files in os.walk(_root_dir):
        for d in dirs:
            fdir = Path(os.path.join(subdir, d))

            if skip_dunder_dirs and d == _dest_folder_name:
                if [f for f in fdir.glob('*') if f.is_file()]:
                    cleanup.append(str(fdir).replace(_root_dir.as_posix(), ''))
                continue
            elif skip_dunder_dirs and (d.startswith('.') or d.startswith('__..') or d.startswith('_.')):
                continue
            elif d == '___hbscripter' or '___hbscripter' in subdir:
                continue
            elif skip_dunder_dirs and '__..' in subdir:
                continue

            sdirs.append(fdir)
    return (sdirs, cleanup)


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

        if _args.clean:
            return

        for d in scanDirs:
            scan_dir(d)

        if _single_queue is not None:
            print(len(_single_queue))
            _file_sorter(lambda x: x.shortDir, _single_queue)
            write_queue(_single_queue, Path(_root_dir))


run()
