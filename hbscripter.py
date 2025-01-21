import glob
import sys
import os
import pwd
import grp
import stat
import re
import datetime
import traceback
import math
import json
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
# _rx_enc_settings_strip = re.compile(r'(~.+)|((-\d+)?-nvenc-[cq\d]+)')
_rx_enc_settings_strip = re.compile(r'~.+')
_rx_enc_res_strip = re.compile(r'-nvenc-.+')

_file_filters = {
    'converted': _rx_converted
}

_dir_filters = {

}

ap = ArgumentParser()
ap.add_argument("-t", "--trace", action='store_true', help="Ignore FPs Factor error")
ap.add_argument("-rd", "--root-dir", type=str, help="Root directory")
ap.add_argument("-rm", "--root-map", type=str, help="Replace root path segment ([root segment]:[replacement segment])")
ap.add_argument('-nr', "--non-recursive", action='store_true', help="Only scan the root dir")
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
ap.add_argument("-len", "--list-length", action='store_true', help="List out video length")
ap.add_argument("-fs", "--list-folder-summaries", action='store_true', help="List details by folder")
ap.add_argument("--excl-ungrp", action='store_true', help="Exclude ungrouped files")
ap.add_argument("-minmb", "--min-mbytes", type=int, default=-1, help="Min file size in MB")
ap.add_argument("-ff", "--file-filter", type=str, choices=_file_filters.keys(), help="File filter")
ap.add_argument("-df", "--dir-filter", type=str, choices=_dir_filters.keys(), help="Directory filter")
ap.add_argument("-ns", "--nautilus-sort", action='store_true', help="Sort like Nautilus file browser")
ap.add_argument("--sort-test", action='store_true', help="Test file sorter")
ap.add_argument("--no-bar", action='store_true', help="Don't use progress bar")
_args = ap.parse_args()

# takes a while, so avoid if --help called
import cv2


def windows_sorter(f: Callable[[Any], str], iterr: List, parent: Path = None):
    iterr.sort(key=lambda x: enc.windows_file_sort_keys(f(x)))


def nautilus_sorter(f: Callable[[Any], str], iterr: List, parent: Path = None):
    iterr.sort(key=lambda x: f(x).strip('/').strip('_').casefold())


def dblcmd_sorter(f: Callable[[Any], str], iterr: List, parent: Path = None):
    iterr.sort(key=lambda x: enc.dblcmd_file_sort_keys(f(x), parent))


_max_fps = 30
_max_bitrate = 5
_single_queue = []
_dest_folder_name = '__..c'
_nobody_uid = pwd.getpwnam("nobody").pw_uid
_users_gid = grp.getgrnam("users").gr_gid
_min_bytes = _args.min_mbytes * 1048576 if _args.min_mbytes > 0 else -1

_extensions = {
    '.mp4': enc.defaultBitrateMod,
    '.mov': enc.defaultBitrateMod,
    '.ts': enc.defaultBitrateMod,
    '.avi': enc.defaultBitrateMod,
    '.mkv': enc.defaultBitrateMod,
    '.mkvv': enc.defaultBitrateMod,
    '.wmv': enc.wmvBitrateMod,
    '.m4v': enc.defaultBitrateMod,
    '.mpg': enc.defaultBitrateMod,
    '.flv': enc.defaultBitrateMod,
    '.webm': enc.defaultBitrateMod,
    '.vid': enc.defaultBitrateMod,
    '.f4v': enc.defaultBitrateMod
}

_root_map = None
_list_details = _args.list_fps or _args.list_fps_error or _args.list_bitrate or _args.list_bitrate_error or _args.list_length
_list_error_only = _args.list_fps_error or _args.list_bitrate_error
_list_fps = _args.list_fps or _args.list_fps_error
_list_bitrate = _args.list_bitrate or _args.list_bitrate_error
_file_filter = None
_dir_filter = None
_file_sorter: Callable[[Callable[[Any], str], List, Path], None] = nautilus_sorter if _args.nautilus_sort else windows_sorter if _args.win else dblcmd_sorter
_root_dir = Path(_args.root_dir).resolve() if _args.root_dir else Path('./').resolve()
_set_title = 'title' if _args.win else 'set_title'
_script_preamble = '' if _args.win else '''#!/bin/bash

function set_title() {
  echo -e "\033]0;$1\007";
}

'''

if _args.ignore_fps_factor:
    enc.ignore_fps_factor()

if not _root_dir.exists():
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
    _max_bitrate = int(_args.bitrate_limit)


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
def print_table(data: List[Dict[str, Union[str, int]]],
                col_order: List[str] = None,
                tablefmt: str = None,
                data_row_color: str = None,
                prefixes: List[str] = None,
                unset_cell_color: str = shellcolors.BLACK,
                show_headers: bool = True):
    if not tablefmt:
        tablefmt = 'simple' if show_headers else 'plain'

    headers = []
    rows = []

    for d in data:
        if isinstance(d, str):
            continue
        for k in d.keys():
            if not k.startswith('_') and (not col_order or k in col_order) and k not in headers:
                headers.append(k)

    if col_order:
        for k in list(col_order):
            if k not in headers:
                col_order.remove(k)

        headers = col_order

    for d in data:
        if isinstance(d, str):
            rows.append([d])
            continue

        row = []
        row_color = data_row_color

        def add_cell(cell_value):
            pref = ''
            cell_value = str(cell_value)

            if prefixes:
                clear = False

                while not clear:
                    clear = True

                    for p in prefixes:
                        if cell_value.startswith(p):
                            pref += p
                            cell_value = cell_value.replace(p, '')
                            clear = False
            if pref:
                pref = f'{data_row_color}{pref}{shellcolors.OFF}'
            row.append(f'{pref}{row_color}{cell_value.replace(unset_cell_color, row_color)}{shellcolors.OFF}')

        if '_rowcolor' in d:
            row_color = d['_rowcolor']

        for k in col_order:
            if k in d:
                add_cell(d[k])
            else:
                add_cell('')

        rows.append(row)

    print(tabulate(rows, headers=headers if show_headers else (), tablefmt=tablefmt))


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
            quality = ''
            fps = ''

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

            if f.setfps and not f.setfps == 0:
                cmd += f' -r {f.setfps} --pfr'

            if f.setfps:
                fps = f'-r{f.setfps}'

            if _root_map:
                source_path = Path(f.sourcePath.as_posix().replace(_root_map[0], _root_map[1]))
                dest_folder = Path(b.destFolder.as_posix().replace(_root_map[0], _root_map[1]))
            else:
                source_path = f.sourcePath
                dest_folder = b.destFolder

            cmd += f' -i {cmd_path_map(source_path)}'
            enc_suffix = f'-nvenc{quality}{fps}'
            title = escape_shell_str(f.name)

            if f.times:
                ti = 0
                for t in f.times:
                    qi += 1
                    cnt = f'-{ti}' if f.multiTimes else ''
                    dest_path = dest_folder / f'{f.name}{cnt}{enc_suffix}.mp4'
                    title_cmd = f'{_set_title} "{qi}/{tot} {title}" && ' if _set_title else ''
                    cmds.append(f'{title_cmd}{cmd} -o {cmd_path_map(dest_path)} --start-at seconds:{t.start} --stop-at seconds:{t.length}')
                    ti += 1
            else:
                qi += 1
                dest_path = dest_folder / f'{f.name}{enc_suffix}.mp4'
                title_cmd = f'{_set_title} "{qi}/{tot} {title}" && ' if _set_title else ''
                cmds.append(f'{title_cmd}{cmd} -o {cmd_path_map(dest_path)}')

            if _args.win:
                cmds.append(f'move /y {cmd_path_map(source_path)} {cmd_path_map(dest_folder / f.fileName)}')
            else:
                # if Path(f.fileName).suffix == '.mkv':
                #     dest1 = dest_folder / f'{f.fileName}v'
                #     dest2 = dest_folder / f'{f.fileName}'
                #     # cmds.append(f'mv {cmd_path_map(source_path)} {cmd_path_map(dest1)} && mv {cmd_path_map(dest1)} {cmd_path_map(dest2)}')
                #     cmds.append(f'mv {cmd_path_map(source_path)} {cmd_path_map(dest1)}')
                # else:
                #     cmds.append(f'mv {cmd_path_map(source_path)} {cmd_path_map(dest_folder / f.fileName)}')

                cmds.append(f'mv {cmd_path_map(source_path)} {cmd_path_map(dest_folder / f.fileName)}')

    if cmds:
        if _set_title:
            cmds.append(f'{_set_title} "Queue Completed"')
        qfp = os.path.join(queue_dir, 'queue.bat' if _args.win else 'queue.sh')
        qf = open(qfp, "w")
        qf.truncate()
        qf.write(_script_preamble + (" && ^\n" if _args.win else " &&\n").join(cmds))
        qf.close()
        st = os.stat(qfp)
        os.chmod(qfp, st.st_mode | stat.S_IEXEC)


def flag_file(ef: enc.EncodeConfig):
    if ef.exclude:
        return shellcolors.FAIL + f'!{ef.excludeReason} {ef.name}'
    if not ef.setfps == 0 and ef.fps > 35 and not ef.setfps:
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
    short_dir = full_dir.relative_to(_root_dir).as_posix()
    log_trace(f'Checking {short_dir}')
    files = [f for f in full_dir.glob('*') if f.is_file() and f.suffix.lower() in _extensions.keys()]
    configs = dict([(c.stem, c) for c in full_dir.glob('*') if c.is_file() and c.suffix.lower() == '.json'])
    optfiles = full_dir.glob('_.*')

    for c in configs:
        with open(configs[c]) as cf:
            configs[c] = json.load(cf)

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

            ext = f.suffix.lower()
            clean_path = str(f).replace(_root_dir.as_posix(), '')

            valid_cfg = False

            if f.name in configs:
                cf = configs[f.name]

                name = f.stem
                times = cf['times']
                cfcq = cf['cq']
                enc_bitrate = cfcq if cfcq else ''
                valid_cfg = True
            else:
                ms = _rx_times_str.search(f.stem)
                renc_ms = _rx_renc_str.search(f.stem)

                if ms or renc_ms or _args.renc:
                    log_trace(f'ms: {ms}')
                    log_trace(f'renc_ms: {renc_ms}')

                    if renc_ms:
                        log_trace(f'file options renc_ms')
                        name = renc_ms.group(1)
                        times = 'renc'
                        enc_bitrate = renc_ms.group(4)
                    elif not ms and _args.renc:
                        log_trace(f'file options not ms and _args.renc')
                        name = f.stem
                        times = 'renc'
                        enc_bitrate = ''
                    else:
                        log_trace(f'file options else {ms.groups()}')
                        name = ms.group(1)
                        times = ms.group(2)
                        enc_bitrate = ms.group(4)

                    valid_cfg = True

            if not valid_cfg:
                continue

            try:
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

                log_trace(f'enc_bitrate: {enc_bitrate}')
                ec = enc.EncodeConfig(full_dir, dest_folder, f.name, name, times, vlen, fps, bitrate, ext, cq, enc_bitrate, mcq, mxcq)
                log_trace(f'self.targetCq: {ec.targetCq}')
                log_trace(f'self.setfps: {ec.setfps}')

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


# List headers
class LH:
    dir_hdr = 'dir'
    path_hdr = 'path'
    files_hdr = 'files'
    fullpath_hdr = 'fullpath'
    fps_hdr = 'fps'
    bitrate_hdr = 'bitrate'
    len_hdr = 'length'


def file_details(f: Path, get_fps, get_bitrate, get_length) -> Dict[str, Union[str, int]]:
    datum = {LH.path_hdr: f.name, '_stem': f.stem, '_include': False}

    ms1 = _rx_enc_settings_strip.search(f.stem)
    ms2 = _rx_enc_res_strip.search(f.stem)

    if ms1:
        datum['_grp_enc'] = _rx_enc_settings_strip.sub('', f.stem)
        datum['_grp_res'] = None
    elif ms2:
        datum['_grp_enc'] = None
        datum['_grp_res'] = _rx_enc_res_strip.sub('', f.stem)
    else:
        datum['_grp_enc'] = None
        datum['_grp_res'] = f.stem

    v = cv2.VideoCapture(str(f))
    fps = v.get(cv2.CAP_PROP_FPS)
    frames = int(v.get(cv2.CAP_PROP_FRAME_COUNT))

    if fps == 0:
        vlen = None
    else:
        vlen = math.ceil(frames / fps) + 1

    if get_fps:
        above_threshold = fps > _max_fps
        datum['_fps_exc'] = above_threshold

        if above_threshold:
            datum['_rowcolor'] = shellcolors.FAIL

        if (not _args.list_fps_error and not _list_error_only) or (_args.list_fps_error and above_threshold):
            datum['_include'] = True

        datum[LH.fps_hdr] = fps

    if get_bitrate:
        if vlen:
            vmb = (f.stat().st_size / 1000000) * 8
            bitrate = round(vmb / vlen, 1)

            above_threshold = bitrate > _args.bitrate_limit
            datum['_btr_exc'] = above_threshold

            if bitrate > _max_bitrate:
                datum['_rowcolor'] = shellcolors.FAIL

            if (not _args.list_bitrate_error and not _list_error_only) or (_args.list_bitrate_error and above_threshold):
                datum['_include'] = True

            datum[LH.bitrate_hdr] = bitrate
        else:
            datum['_include'] = True
            datum[LH.bitrate_hdr] = f'{shellcolors.FAIL}ERR{shellcolors.OFF}'

    if _args.list_length:
        if not _list_error_only:
            datum['_include'] = True

        if vlen:
            td = datetime.timedelta(seconds=vlen)
            datum[LH.len_hdr] = str(td).lstrip('0:')
        else:
            datum[LH.len_hdr] = f'{shellcolors.FAIL}ERR{shellcolors.OFF}'

    if _args.trace:
        datum['fullpath'] = f

    return datum


def list_details(dirs: Union[List[Path], Path], file_count):
    expanded_table = True if sum([_list_fps, _list_bitrate]) > 1 or _args.list_folder_summaries else False
    folder_summaries_only = True if sum([_list_fps, _list_bitrate]) < 1 else False
    data: List[Union[Dict[str, Union[str, int]], str]] = []
    max_fps_hdr = f'fps > {_max_fps}'
    max_bitrate_hdr = f'btr > {_max_bitrate}'
    col_order = [LH.dir_hdr, LH.path_hdr, LH.fps_hdr, LH.bitrate_hdr, LH.len_hdr, LH.files_hdr, max_fps_hdr, max_bitrate_hdr, LH.fullpath_hdr]

    if isinstance(dirs, Path):
        if dirs.is_file():
            fd = file_details(dirs, _list_fps or folder_summaries_only, _list_bitrate or folder_summaries_only, _args.list_length)
            print_table([fd], data_row_color=shellcolors.OKGREEN, col_order=col_order, show_headers=sum([_list_fps, _list_bitrate]) > 1)
            return
        else:
            print('Bad args combo')
            return

    def clean_path(p: Path):
        return p.relative_to(_root_dir).as_posix()

    _file_sorter(lambda x: clean_path(x), dirs, _root_dir)

    if not _args.no_bar and file_count > 30:
        pbar = tqdm(total=file_count, desc='Scanning files')
    else:
        pbar = None
        if _args.no_bar:
            print('Scanning files')

    fld_count = 0

    for d in dirs:
        groupings: List[Union[str, List[Dict[str, Union[str, int]]]]] = []

        dir_clean = clean_path(d)

        if not dir_clean:
            dir_clean = '[root]'

        files = [f for f in d.glob('*') if f.is_file() and f.suffix.lower() in _extensions.keys()]
        _file_sorter(lambda x: x.name, files, d)

        fld_datum = {LH.dir_hdr: dir_clean, '_fld_datum': True, '_include': False}

        for f in files:
            if _file_filter and not re.search(_file_filter, f.name, flags=re.IGNORECASE):
                if pbar:
                    pbar.update()
                continue
            if f.stat().st_size < _min_bytes:
                if pbar:
                    pbar.update()
                continue

            if _args.list_folder_summaries:
                if LH.files_hdr not in fld_datum:
                    fld_datum[LH.files_hdr] = 1
                else:
                    fld_datum[LH.files_hdr] += 1

            datum = file_details(f, _list_fps or folder_summaries_only, _list_bitrate or folder_summaries_only, _args.list_length)

            if _list_fps or folder_summaries_only:
                if max_fps_hdr not in fld_datum:
                    fld_datum[max_fps_hdr] = 0

                if datum['_fps_exc']:
                    if folder_summaries_only:
                        fld_datum['_rowcolor'] = shellcolors.FAIL

                    fld_datum[max_fps_hdr] += 1

            if _list_bitrate or folder_summaries_only:
                if max_bitrate_hdr not in fld_datum:
                    fld_datum[max_bitrate_hdr] = 0

                if '_btr_exc' in datum and datum['_btr_exc']:
                    if folder_summaries_only:
                        fld_datum['_rowcolor'] = shellcolors.FAIL

                    fld_datum[max_bitrate_hdr] += 1

            if datum['_include'] or folder_summaries_only:
                fld_datum['_include'] = True

            if pbar:
                pbar.update()

            if not folder_summaries_only and datum['_include']:
                if not groupings:
                    groupings.append([datum['_grp_enc'] or '', datum])
                else:
                    prev_grouping = groupings[-1]

                    if datum['_grp_enc']:
                        grouping = []

                        if prev_grouping[0] == '':
                            while True:
                                if isinstance(prev_grouping[-1], str):
                                    prev_grouping.pop()
                                    break
                                elif prev_grouping[-1]['_grp_res'] and prev_grouping[-1]['_grp_res'].startswith(datum['_grp_enc']):
                                    prev_datum = prev_grouping.pop()
                                    prev_datum['_grp_enc'] = datum['_grp_enc']
                                    grouping.append(prev_datum)
                                else:
                                    break

                            _file_sorter(lambda dt: dt[LH.path_hdr], grouping)

                        if not prev_grouping:
                            groupings.pop()

                        grouping.insert(0, datum['_grp_enc'])
                        grouping.append(datum)
                        groupings.append(grouping)
                    else:
                        if not prev_grouping[0] or datum['_grp_res'].startswith(prev_grouping[0]):
                            prev_grouping.append(datum)
                        else:
                            groupings.append(['', datum])

        if fld_datum['_include']:
            fld_count += 1

        if _args.trace:
            fld_datum['fullpath'] = d

        if _args.list_folder_summaries:
            data.append(fld_datum)

        if not folder_summaries_only and fld_datum['_include']:
            if not _args.list_folder_summaries:
                data.append({LH.dir_hdr: shellcolors.OKBLUE + fld_datum[LH.dir_hdr] + shellcolors.OFF, '_fld_datum': True})
            else:
                fld_datum[LH.dir_hdr] = shellcolors.OKBLUE + fld_datum[LH.dir_hdr] + shellcolors.OFF
                fld_datum[LH.path_hdr] = fld_datum[LH.files_hdr]
                fld_datum.pop(LH.files_hdr)
                if _list_fps:
                    if fld_datum[max_fps_hdr] == 0:
                        fld_datum[LH.fps_hdr] = fld_datum[max_fps_hdr]
                    else:
                        fld_datum[LH.fps_hdr] = f'{shellcolors.FAIL}{str(fld_datum[max_fps_hdr])}{shellcolors.OFF} > {_max_fps}'
                    fld_datum.pop(max_fps_hdr)
                if _list_bitrate:
                    if fld_datum[max_bitrate_hdr] == 0:
                        fld_datum[LH.bitrate_hdr] = fld_datum[max_bitrate_hdr]
                    else:
                        fld_datum[LH.bitrate_hdr] = f'{shellcolors.FAIL}{str(fld_datum[max_bitrate_hdr])}{shellcolors.OFF} > {_max_bitrate}'
                    fld_datum.pop(max_bitrate_hdr)

            grp_data = []
            grp_sep = ' '

            for g in groupings:
                gn = g.pop(0)

                if _args.excl_ungrp and not gn:
                    continue

                if len(g) > 1:
                    if grp_data and not isinstance(grp_data[-1], str):
                        grp_data.append(grp_sep)
                    grp_data = grp_data + g
                    grp_data.append(grp_sep)
                    if not _args.excl_ungrp and gn:
                        for dt in g:
                            dt[LH.path_hdr] = '| ' + dt[LH.path_hdr]
                else:
                    grp_data = grp_data + g

            if grp_data:
                if isinstance(grp_data[-1], str):
                    grp_data.pop()

                data = data + grp_data
            else:
                data.pop()

    if fld_count < 2 and not expanded_table:
        data = list(filter(lambda d: '_fld_datum' not in d or not d['_fld_datum'], data))

    if pbar:
        pbar.close()

    if data:
        print()
        print_table(data, data_row_color=shellcolors.OKGREEN, col_order=col_order, prefixes=['| '], show_headers=expanded_table)
    else:
        log('No data to list')

    print('\n')


def scan_dirs(skip_dunder_dirs=True) -> Tuple[List[Path], int, List[Path]]:
    log(f'Scanning {_root_dir}')
    sdirs = [_root_dir]
    cleanup = []
    file_count = 0

    if not _args.non_recursive:
        for subdir, dirs, files in os.walk(_root_dir):
            file_count += len(files)
            for d in dirs:
                fdir = Path(os.path.join(subdir, d))

                if _dir_filter and not re.search(_dir_filter, str(d), flags=re.IGNORECASE):
                    continue
                elif skip_dunder_dirs and d == _dest_folder_name:
                    if [f for f in fdir.glob('*') if f.is_file()]:
                        cleanup.append(fdir)
                    continue
                elif skip_dunder_dirs and (d.startswith('.') or d.startswith(_dest_folder_name) or d.startswith('_.')):
                    continue
                elif d == '___hbscripter' or '___hbscripter' in subdir:
                    continue
                elif skip_dunder_dirs and '__..' in subdir:
                    continue

                sdirs.append(fdir)
    else:
        file_count += len(list(_root_dir.glob('*')))

    return (sdirs, file_count, cleanup)


def run():
    if _list_details:
        if _root_dir.is_file():
            list_details(_root_dir, 1)
        else:
            (scanDirs, file_count, cleanup) = scan_dirs(skip_dunder_dirs=False)
            list_details(scanDirs, file_count)
    else:
        (scanDirs, file_count, cleanup) = scan_dirs()

        _file_sorter(lambda x: str(x), scanDirs, _root_dir)
        if cleanup:
            clean_dirs: List[str] = list(map(lambda p: p.relative_to(_root_dir).as_posix(), cleanup))
            _file_sorter(lambda x: x, clean_dirs, _root_dir)
            lst = '\n\t'.join(clean_dirs)
            log(f'Media to cleanup:\n\t{lst}\n', shellcolors.OKGREEN)

        if _args.clean:
            return

        for d in scanDirs:
            scan_dir(d)

        if _single_queue is not None:
            print(len(_single_queue))
            _file_sorter(lambda x: x.shortDir, _single_queue, _root_dir)
            write_queue(_single_queue, Path(_root_dir))


if __name__ == '__main__':
    if _args.sort_test:
        files = [Path(f) for f in glob.glob('./windows_sorting/*.txt')]
        _file_sorter(lambda f: f.name, files, _root_dir)

        for f in files:
            print(f.name)
    else:
        run()
