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
from tabulate import tabulate
from tqdm import tqdm

from typing import List, Any, Dict, Tuple, Callable, Union

import encodingCommon as enc

shellcolors = enc.shellcolors

_rx_options = r'( \[(([mqxr]*?[0-9.]+ ?)*?)\])*'
_rx_times_str = re.compile(r'^(.+)~([\d;:\- ]+)' + _rx_options + '$')
_rx_renc_str = re.compile(r'^(.+)~(renc)' + _rx_options + '$')
_rx_converted = re.compile(r'(-\d+)?(-nvenc-[cq\d]+)')
_rx_enc_settings_strip = re.compile(r'(~.+)|((-\d+)?-nvenc-[cq\d]+)')

_file_filters = {
    'converted': _rx_converted
}

_dir_filters = {

}

ap = ArgumentParser()
ap.add_argument("-t", "--trace", action='store_true', help="Ignore FPs Factor error")
ap.add_argument("-rd", "--root-dir", type=str, help="Root directory")
ap.add_argument("-rm", "--root-map", type=str, help="Replace root path segment ([root segment]:[replacement segment])")
ap.add_argument("-tbr", "--target-bitrate", type=str, help="Target bitrate")
ap.add_argument('-win', "--win", action='store_true', help="Write queue for windows")
ap.add_argument("--plan", action='store_true', help="Don't write queue")
ap.add_argument("--clean", action='store_true', help="Show folders needing cleaning")
ap.add_argument("-iff", "--ignore-fps-factor", action='store_true', help="Ignore FPs Factor error")
ap.add_argument("-renc", "--renc", action='store_true', help="Reencode all")
ap.add_argument("-fps", "--list-fps", action='store_true', help="List FPS details")
ap.add_argument("-fpse", "--list-fps-error", action='store_true', help="List out of bounds FPS")
ap.add_argument("-btrl", "--bitrate-limit", type=int, default=5, help="Override bitrate upper bound")
ap.add_argument("-btr", "--list-bitrate", action='store_true', help="List bitrate details")
ap.add_argument("-btre", "--list-bitrate-error", action='store_true', help="List out of bounds bitrates")
ap.add_argument("-fs", "--list-folder-summaries", action='store_true', help="List details by folder")
ap.add_argument("-minmb", "--min-bytes", type=int, default=-1, help="Min file size in MB")
ap.add_argument("-ff", "--file-filter", type=str, choices=_file_filters.keys(), help="File filter")
ap.add_argument("-df", "--dir-filter", type=str, choices=_dir_filters.keys(), help="Directory filter")
ap.add_argument("-ns", "--nautilus-sort", type=str, choices=_dir_filters.keys(), help="Sort like Nautilus file browser")
_args = ap.parse_args()

# takes a while, so avoid if --help called
import cv2


def windows_sorter(f: Callable[[Any], str], iterr: List):
    iterr.sort(key=lambda x: enc.windows_file_sort_keys(f(x)))


def nautilus_sorter(f: Callable[[Any], str], iterr: List):
    iterr.sort(key=lambda x: f(x).strip('/').strip('_').casefold())


def dblcmd_sorter(f: Callable[[Any], str], iterr: List):
    iterr.sort(key=lambda x: enc.dblcmd_file_sort_keys(f(x)))


_max_fps = 30
_bitrate_limit = 5
_single_queue = []
_dest_folder_name = '__..c'
_nobody_uid = pwd.getpwnam("nobody").pw_uid
_users_gid = grp.getgrnam("users").gr_gid

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
_list_details = _args.list_fps or _args.list_fps_error or _args.list_bitrate or _args.list_bitrate_error
_list_fps = _args.list_fps or _args.list_fps_error
_list_bitrate = _args.list_bitrate or _args.list_bitrate_error
_file_filter = None
_dir_filter = None
_file_sorter = nautilus_sorter if _args.nautilus_sort else windows_sorter if _args.win else dblcmd_sorter
_root_dir = Path(_args.root_dir).resolve() if _args.root_dir else Path('./').resolve()
_set_title = 'title' if _args.win else 'set_title'
_script_preamble = '' if _args.win else '''#!/bin/bash

function set_title() {
  echo -e "\033]0;$1\007";
}

'''

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

if _args.bitrate_limit:
    _bitrate_limit = int(_args.bitrate_limit)


def logts() -> str:
    return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def error(msg) -> None:
    print(f'{shellcolors.FAIL}{logts()}\tERR\t{msg}{shellcolors.OFF}')


def log(msg, color='') -> None:
    print(f'{color or (shellcolors.OKBLUE if _args.trace else "")}{logts()}\tLOG\t{msg}{shellcolors.OFF}')


def log_trace(msg) -> None:
    if _args.trace:
        print(f'{logts()}\tTRC\t{msg}')


# https://github.com/astanin/python-tabulate
def print_table(data: List[Union[List[Union[str, int]], Dict[str, Union[str, int]]]],
                headers: Union[str, List[str]] = None,
                col_order: List[str] = None,
                tablefmt: str = 'plain',
                data_row_color: str = None):
    generate_headers = False

    if headers == 'keys':
        generate_headers = True
    elif headers is None:
        headers = ()

    data_types = list(set(map(lambda x: type(x), data)))

    data_types = list(filter(lambda t: t is not str, data_types))

    if len(data_types) > 1:
        print('Cannot print table of mixed list and dict')
        sys.exit(1)

    is_dict_data = data_types[0] == dict

    rows = []

    if (not col_order or '_.' in col_order) and is_dict_data:
        col_order = col_order if col_order else []
        for d in data:
            if type(d) is str:
                continue
            for k in d.keys():
                if not k.startswith('_') and k not in col_order:
                    col_order.append(k)

    if generate_headers:
        headers = col_order

    for d in data:
        if type(d) is str:
            rows.append([d])
            continue

        row = []
        row_color = data_row_color

        def add_cell(c):
            row.append(f'{row_color}{c}{shellcolors.OFF}')

        if is_dict_data:
            if '_rowcolor' in d:
                row_color = d['_rowcolor']

            for k in col_order:
                if k in d:
                    add_cell(d[k])
                else:
                    add_cell('')
        else:
            for c in d:
                add_cell(c)

        rows.append(row)

    print(tabulate(rows, headers=headers, tablefmt=tablefmt))


enc.init(_extensions, error)


def escape_shell_str(obj):
    if _args.win:
        return str(obj).replace('/', '\\').replace('\\mnt\\user\\', '\\\\rmofrequirement.local\\')
    else:
        return str(obj).replace('"', '""').replace('$', r'\$')


def cmd_path_map(path: Path):
    return '"' + escape_shell_str(path) + '"'


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

            if _args.win:
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
                    title = f'{_set_title} "{qi}/{tot} {escape_shell_str(f.name)}" && ' if _set_title else ''
                    cmds.append(f'{title}{cmd} -o {cmd_path_map(dest_path)} --start-at seconds:{t.start} --stop-at seconds:{t.length}')
                    ti += 1
            else:
                qi += 1
                dest_path = dest_folder / f'{f.name}-nvenc{quality}.mp4'
                title = f'{_set_title} "{qi}/{tot} {escape_shell_str(f.name)}" && ' if _set_title else ''
                cmds.append(f'{title}{cmd} -o {cmd_path_map(dest_path)}')

            if _args.win:
                cmds.append(f'move {cmd_path_map(source_path)} {cmd_path_map(dest_folder / f.fileName)}')
            else:
                cmds.append(f'mv {cmd_path_map(source_path)} {cmd_path_map(dest_folder / f.fileName)}')

    if cmds:
        if _set_title:
            cmds.append(f'{_set_title} "Queue Completed"')
        qfp = os.path.join(queue_dir, 'queue.txt' if _args.win else 'queue.sh')
        qf = open(qfp, "w")
        qf.truncate()
        qf.write(_script_preamble + (" && ^\n" if _args.win else " &&\n").join(cmds))
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
            ms = _rx_times_str.search(f.stem)
            renc_ms = _rx_renc_str.search(f.stem)

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


def list_file_grouping(path):
    return re.sub(_rx_enc_settings_strip, '', Path(path).stem)


def list_details(dirs):
    expanded_table = True if sum([_list_fps, _list_bitrate]) > 1 else False
    data: List[Union[Dict[str, Union[str, int]], str]] = []
    dir_hdr = 'dir'
    path_hdr = 'path'
    files_hdr = 'files'
    fullpath_hdr = 'fullpath'
    max_fps_hdr = f'> {_max_fps}'
    bitrate_limit_hdr = f'> {_bitrate_limit}'
    col_order = [dir_hdr, path_hdr, files_hdr, max_fps_hdr, bitrate_limit_hdr, fullpath_hdr]

    _file_sorter(lambda x: str(x), dirs)
    iterr = tqdm(dirs, desc='Scanning files') if len(dirs) > 2 else dirs

    for d in iterr:
        groupings: List[List[Dict[str, Union[str, int]]]] = []

        if _dir_filter and not re.search(_dir_filter, str(d), flags=re.IGNORECASE):
            continue

        dir_clean = re.sub(r'^/', '', str(d).replace(_root_dir.as_posix(), ''))

        if not dir_clean:
            dir_clean = '[root]'

        files = [f for f in d.glob('*') if f.is_file() and f.suffix.lower() in _extensions.keys()]
        _file_sorter(lambda x: x.name, files)
        fld_datum = {dir_hdr: dir_clean, '_fld_datum': True}

        if _args.list_folder_summaries:
            data.append(fld_datum)
        else:
            data.append(shellcolors.OKBLUE + dir_clean)

        for f in files:
            if _file_filter and not re.search(_file_filter, f.name, flags=re.IGNORECASE):
                continue
            if f.stat().st_size < _args.min_bytes:
                continue

            clean_path = re.sub(r'^/', '', str(f).replace(_root_dir.as_posix(), '').replace(fld_datum[dir_hdr], '').strip('/'))
            datum = {'path': '\t' + clean_path, '_grp': list_file_grouping(clean_path)}

            v = cv2.VideoCapture(str(f))

            if _args.list_folder_summaries:
                if 'files' not in fld_datum:
                    fld_datum['files'] = 1
                else:
                    fld_datum['files'] += 1

            if _list_fps:
                fps = v.get(cv2.CAP_PROP_FPS)

                if _args.list_folder_summaries:
                    if fps > _max_fps:
                        fld_datum['_rowcolor'] = shellcolors.FAIL

                        if max_fps_hdr not in fld_datum:
                            fld_datum[max_fps_hdr] = 1
                        else:
                            fld_datum[max_fps_hdr] += 1
                else:
                    if fps > _max_fps:
                        datum['_rowcolor'] = shellcolors.FAIL

                    if not _args.list_fps_error or fps > _max_fps:
                        datum['fps'] = fps

            if _list_bitrate > 0:
                fps = v.get(cv2.CAP_PROP_FPS)
                frames = int(v.get(cv2.CAP_PROP_FRAME_COUNT))
                vlen = math.ceil(frames / fps) + 1
                vmb = (f.stat().st_size / 1000000) * 8
                bitrate = round(vmb / vlen, 1)

                if _args.list_folder_summaries:
                    if bitrate > _bitrate_limit:
                        fld_datum['_rowcolor'] = shellcolors.FAIL

                        if bitrate_limit_hdr not in fld_datum:
                            fld_datum[bitrate_limit_hdr] = 1
                        else:
                            fld_datum[bitrate_limit_hdr] += 1
                else:
                    if bitrate > _args.bitrate_limit:
                        datum['_rowcolor'] = shellcolors.FAIL

                    datum['bitrate'] = bitrate

            if _args.trace:
                datum['fullpath'] = f

            if not _args.list_folder_summaries:
                if (not groupings) or (not groupings[-1][0]['_grp'] == datum['_grp']):
                    groupings.append([])
                groupings[-1].append(datum)

        if _args.trace:
            fld_datum['fullpath'] = d

        grp_data = []
        grp_sep = ' '

        for g in groupings:
            if len(g) > 1:
                if grp_data and type(grp_data[-1]) is not str:
                    grp_data.append(grp_sep)
                grp_data = grp_data + g
                grp_data.append(grp_sep)
            else:
                grp_data = grp_data + g

        if grp_data:
            if type(grp_data[-1]) is str:
                grp_data.pop()

            data = data + grp_data
        else:
            data.pop()

    print_table(data, headers='keys' if expanded_table else None, data_row_color=shellcolors.OKGREEN)

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
