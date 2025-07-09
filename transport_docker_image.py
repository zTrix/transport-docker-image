#!/usr/bin/env python
from typing import List, Dict, Any, Optional
import os
import sys
import json
import random
import string
import logging
import argparse
import getpass
import shlex
import subprocess
import time
from urllib.parse import urlparse, parse_qs, quote_plus

import paramiko

logger = logging.getLogger('TransDockerImage')

clean_template = """
#!/usr/bin/env python

import os
import sys
import shutil
import json

os.chdir(os.path.dirname(os.path.realpath(__file__)))

image_name = '''
$image_name
'''.strip()

existing_layers = $existing_layers

with open('manifest.json') as f:
    manifest = json.loads(f.read())

for item in manifest:
    if image_name in item['RepoTags']:
        layers = item['Layers']
        with open(item['Config']) as f:
            config = json.loads(f.read())
        
        for i, e in enumerate(config['rootfs']['diff_ids']):
            if e in existing_layers:
                layer = layers[i]
                if os.path.isfile(layer):
                    print('removing ' + layer, file=sys.stderr)
                    os.unlink(layer)
                else:
                    print('removing ' + os.path.dirname(layer), file=sys.stderr)
                    shutil.rmtree(os.path.dirname(layer), ignore_errors=True)
"""

def rand_str(n=8, charset=None):
    if charset is None:
        charset = string.printable[:62]
    return ''.join([random.choice(charset) for _ in range(n)])

def readable_size(num, use_kibibyte=True, unit_ljust=0):
    base, suffix = [(1000.,'B'),(1024.,'iB')][use_kibibyte]
    for x in ['B'] + [x+suffix for x in 'kMGTP']:
        if -base < num < base:
            return "%3.1f %s" % (num, x.ljust(unit_ljust, ' '))
        num /= base
    return "%3.1f %s" % (num, x.ljust(unit_ljust, ' '))

def parse_image_name(name:str):
    if '@' in name or "ssh://" in name:
        if not name.startswith("ssh://"):
            name = 'ssh://' + name
        parsed = urlparse(name)

        ssh_option = {}

        ssh_option['host'] = parsed.hostname
        if not ssh_option['host']:
            raise Exception('invalid ssh hostname')

        ssh_option['port'] = 22 if not parsed.port else parsed.port

        ssh_option['username'] = getpass.getuser() if not parsed.username else parsed.username
        if parsed.password:
            ssh_option['password'] = parsed.password

        image_with_tag = parsed.path.lstrip('/')
        if not image_with_tag:
            raise Exception('invalid image name')

        if parsed.query:
            mapping = parse_qs(parsed.query)

            if mapping.get('proxy'):
                proxy_host = mapping.get('proxy')[0]
                proxy_user = 'root'
                if '@' in proxy_host:
                    ary = proxy_host.split('@', maxsplit=1)
                    proxy_user = ary[0]
                    proxy_host = ary[1]

                jumpbox = paramiko.SSHClient()
                jumpbox.set_missing_host_key_policy(paramiko.WarningPolicy())
                jumpbox.connect(proxy_host, username=proxy_user)

                jumpbox_transport = jumpbox.get_transport()
                src_addr = ('0.0.0.0', 0)
                dest_addr = (ssh_option['host'], ssh_option['port'])
                jumpbox_channel = jumpbox_transport.open_channel("direct-tcpip", dest_addr, src_addr)

                ssh_option['sock'] = jumpbox_channel

        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.WarningPolicy())
        hostname = ssh_option.pop('host')
        client.connect(hostname, timeout=10, **ssh_option)

        return client, image_with_tag
    else:
        return None, name

def file_size(path, ssh_client:paramiko.SSHClient=None):
    if ssh_client is None:
        return os.stat(path).st_size
    else:
        sftp_client = ssh_client.open_sftp()
        stat = sftp_client.stat(path)
        sftp_client.close()
        return stat.st_size

def open_file(path, ssh_client:paramiko.SSHClient=None, mode='rb'):
    if ssh_client is None:
        return open(path, mode)
    else:
        sftp_client = ssh_client.open_sftp()
        return sftp_client.open(path, mode)

def write_file(path, content:bytes, ssh_client:paramiko.SSHClient=None):
    logger.info('[ STEP ] write file to %s, length = %d' % (path, len(content)))
    if ssh_client is not None:
        sftp_client = ssh_client.open_sftp()
        with sftp_client.open(path, 'wb') as f:
            f.write(content)
        sftp_client.close()
    else:
        with open(path, 'wb') as f:
            f.write(content)

def read_files(path_list:List[str], ssh_client:Optional[paramiko.SSHClient]=None, mode='r', transform:Optional[callable]=None):
    ret = []
    if ssh_client is not None:
        sftp_client = ssh_client.open_sftp()
        for path in path_list:
            with sftp_client.open(path, mode) as f:
                s = f.read()
                if transform is not None: s = transform(s)
                ret.append(s)
        sftp_client.close()
    else:
        for path in path_list:
            with open(path, mode) as f:
                s = f.read()
                if transform is not None: s = transform(s)
                ret.append(s)
    return ret


def list_dir(path:str, ssh_client:paramiko.SSHClient=None):
    if ssh_client is not None:
        sftp_client = ssh_client.open_sftp()
        ret = sftp_client.listdir(path)
        sftp_client.close()
        return ret
    else:
        return os.listdir(path)

def exec_command(command:str, ssh_client:paramiko.SSHClient=None, print_stdout=False, print_stderr=False):
    logger.info('[ %sSTEP ] %s' % ('REMOTE ' if ssh_client is not None else 'LOCAL  ', command))
    if ssh_client is not None:
        stdin_io, stdout_io, stderr_io = ssh_client.exec_command(command)
        stdout = stdout_io.read()
        stderr = stderr_io.read()
        if print_stdout:
            try:
                print(stdout.decode(), file=sys.stderr)
            except:
                print(stdout, file=sys.stderr)
        if print_stderr:
            try:
                print(stderr.decode(), file=sys.stderr)
            except:
                print(stderr, file=sys.stderr)
        return stdout, stderr
    else:
        kwargs = {
            'stdout': subprocess.PIPE,
            'stderr': subprocess.PIPE,
            'shell': True,  # for pipe operator
            # NOTE: use consistent return type(bytes) for exec_command, so do not use utf-8 here
            # 'encoding': 'utf-8',
            'check': True,
        }
        proc = subprocess.run(command, **kwargs)
        if print_stdout:
            try:
                print(proc.stdout.decode(), file=sys.stderr)
            except:
                print(proc.stdout, file=sys.stderr)
        if print_stderr:
            try:
                print(proc.stderr.decode(), file=sys.stderr)
            except:
                print(proc.stderr, file=sys.stderr)
        return proc.stdout, proc.stderr
    
def list_existing_diffid(target_docker_path:str, target_ssh_client:Optional[paramiko.SSHClient], target_image_name:str) -> List[str] | None:
    # METHOD 1: try list all existing diffid in /var/lib/docker/image/overlay2/layerdb/sha256
    try:
        stderr = ""
        stdout, stderr = exec_command('%s info --format "{{json .}}"' % (target_docker_path, ), ssh_client=target_ssh_client, print_stderr=True)
        info_obj:Dict[str, Any] = json.loads(stdout)
        docker_root_dir = info_obj.get("DockerRootDir")
        driver = info_obj.get("Driver")
        if driver == "overlay2":
            diffid_dir = os.path.join(docker_root_dir, "image", "overlay2", "layerdb", "sha256")
            stdout, stderr = exec_command('find %s -type f -name diff -exec cat {} \\; -exec echo \\;' % diffid_dir, ssh_client=target_ssh_client)
            ret = []
            for line in stdout.splitlines():
                line = line.strip().decode('utf-8')
                if line.startswith("sha256:"):
                    ret.append(line)
            return ret
        else:
            logger.info("driver is not overlay2, fallback to inspect target image diff id")
    except Exception:
        logger.exception("could not get docker info, stderr = %r" % stderr)

    stdout, stderr = exec_command('%s inspect %s --format "{{json .RootFS.Layers}}"' % (target_docker_path, shlex.quote(target_image_name)), ssh_client=target_ssh_client, print_stderr=True)

    if (not stdout or not stdout.strip()) and b'no such object:' in stderr.lower():
        logger.error('target image not found at destination, could not shrink size')
        return None
    else:
        existing_layers = json.loads(stdout)
        assert isinstance(existing_layers, list)
        return existing_layers

def main(args):
    if args.workdir:
        tmp_dir = args.workdir
    else:
        tmp_dir = '/tmp/.transport_docker_image/' + rand_str()

    source_ssh_client, source_image_name = parse_image_name(args.source_image)
    target_ssh_client, target_image_name = parse_image_name(args.target_image)

    quoted_source_image_name = quote_plus(source_image_name)

    if source_ssh_client is None and target_ssh_client is None:
        raise Exception('at least one end should be using ssh')

    if args.pre_hook:
        exec_command(args.pre_hook, ssh_client=target_ssh_client, print_stdout=True, print_stderr=True)

    exec_command('mkdir -p %s' % shlex.quote(os.path.join(tmp_dir, quoted_source_image_name)), ssh_client=source_ssh_client)

    stdout, stderr = exec_command('%s save -o %s %s' % (
        args.source_docker_path,
        shlex.quote(os.path.join(tmp_dir, quoted_source_image_name + '.tar')),
        shlex.quote(source_image_name),
    ), ssh_client=source_ssh_client, print_stderr=True, print_stdout=True)
    if stderr and b'error' in stderr.lower():
        raise Exception('failed to save image')

    exec_command('tar -x -f %s -C %s' % (
        shlex.quote(os.path.join(tmp_dir, quoted_source_image_name + '.tar')),
        shlex.quote(os.path.join(tmp_dir, quoted_source_image_name)),
    ), ssh_client=source_ssh_client)

    existing_layers = list_existing_diffid(args.target_docker_path, target_ssh_client=target_ssh_client, target_image_name=target_image_name)

    if existing_layers:
        clean_file = string.Template(clean_template).substitute(existing_layers=json.dumps(existing_layers), image_name=target_image_name)
        write_file(os.path.join(tmp_dir, quoted_source_image_name, 'clean.py'), clean_file.encode(), ssh_client=source_ssh_client)

        exec_command('python %s' % (
            shlex.quote(os.path.join(tmp_dir, quoted_source_image_name, 'clean.py'))
        ), ssh_client=source_ssh_client, print_stderr=True)

        if not args.no_cleanup:
            exec_command('rm -f %s' % (
                shlex.quote(os.path.join(tmp_dir, quoted_source_image_name, 'clean.py'))
            ), ssh_client=source_ssh_client, print_stderr=True)

    shrinked_path = os.path.join(tmp_dir, quoted_source_image_name + '.shrinked.tar.gz')

    if not list_dir(os.path.join(tmp_dir, quoted_source_image_name), ssh_client=source_ssh_client):
        raise Exception('directory is empty')

    exec_command('tar -c %s -f %s -C %s .' % (
        '' if 'podman' in args.source_docker_path else '-z',
        shlex.quote(shrinked_path),
        shlex.quote(os.path.join(tmp_dir, quoted_source_image_name))
    ), ssh_client=source_ssh_client, print_stderr=True)

    exec_command('mkdir -p %s' % shlex.quote(os.path.join(tmp_dir, quoted_source_image_name)), ssh_client=target_ssh_client)

    size = file_size(shrinked_path, ssh_client=source_ssh_client)
    reader = open_file(shrinked_path, mode='rb', ssh_client=source_ssh_client)
    writer = open_file(shrinked_path, mode='wb', ssh_client=target_ssh_client)

    transfered = 0
    transfer_begin_time = time.time()
    chunk_size = args.chunk_size * 1024
    print('transfer started...', file=sys.stderr)
    while True:
        content = reader.read(chunk_size)
        if not content:
            break
        writer.write(content)
        transfered += len(content)
        elapsed = time.time() - transfer_begin_time
        speed = transfered / elapsed if elapsed > 0 else 0
        print('\rtransfered %d/%d, percent = %.2f%%, speed = %s/s    ' % (transfered, size, transfered*100.0/size, readable_size(speed)), end='', file=sys.stderr)

    if transfered == size:
        print('\ntransfer complete, transfered = %d, size = %d' % (transfered, size), file=sys.stderr)
    else:
        print('\n[ WARN ] transfer size mismatch, transfered = %d, size = %d' % (transfered, size), file=sys.stderr)

    reader.close()
    writer.close()

    exec_command('cat %s | %s load' % (
        shlex.quote(shrinked_path),
        args.target_docker_path,
    ), ssh_client=target_ssh_client, print_stdout=True, print_stderr=True)

    if not args.no_cleanup:
        exec_command('rm -f %s %s && rm -rf %s && rmdir %s' % (
            shlex.quote(shrinked_path),
            shlex.quote(os.path.join(tmp_dir, quoted_source_image_name + '.tar')),
            shlex.quote(os.path.join(tmp_dir, quoted_source_image_name)),
            shlex.quote(tmp_dir),
        ), ssh_client=source_ssh_client)

        exec_command('rm -rf %s' % (
            shlex.quote(tmp_dir),
        ), ssh_client=target_ssh_client, print_stdout=True, print_stderr=True)

    if args.post_hook:
        exec_command(args.post_hook, ssh_client=target_ssh_client, print_stdout=True, print_stderr=True)

def str2bool(v):
    if isinstance(v, bool):
       return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

def cli():
    parser = argparse.ArgumentParser(description='Transport docker image with best effort to reduce transmission size')
    parser.add_argument('source_image', help='Source image in format: [user@host[:port]/]image:tag')
    parser.add_argument('target_image', help='Target image in format: [user@host[:port]/]image:tag')
    parser.add_argument('--workdir', help='Specify workdir for tempfile, defaults to random dir under /tmp', required=False)
    parser.add_argument('--loglevel', help='Specify log level', required=False, default=logging.INFO)
    parser.add_argument('--target-docker-path', help='Specify target docker binary path', required=False, default='docker')
    parser.add_argument('--source-docker-path', help='Specify source docker binary path', required=False, default='docker')
    parser.add_argument('--no-cleanup', help='do not cleanup tmp directory after using', type=str2bool, nargs='?', const=True, required=False, default=False)
    parser.add_argument('--pre-hook', help='pre cmd hook before transport starts', type=str, default=None)
    parser.add_argument('--post-hook', help='post cmd hook after transport ended', type=str, default=None)
    parser.add_argument('--chunk-size', help='specify transfer chunk size in KiB', type=int, required=False, default=64)

    args = parser.parse_args()

    logging.basicConfig(level=args.loglevel)
    main(args)

if __name__ == '__main__':
    cli()
