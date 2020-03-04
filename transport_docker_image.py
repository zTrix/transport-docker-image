#!/usr/bin/env python

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
from urllib.parse import urlparse

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

def parse_image_name(name):
    if '@' in name:
        parsed = urlparse('ssh://' + name)

        ssh_option = {}

        ssh_option['host'] = parsed.hostname
        if not ssh_option['host']:
            raise Exception('invalid ssh hostname')

        ssh_option['port'] = 22 if not parsed.port else parsed.port

        ssh_option['username'] = getpass.getuser() if not parsed.username else parsed.username
        if parsed.password:
            ssh_option['password'] = parsed.password

        image_with_tag = parsed.path[1:]
        if not image_with_tag:
            raise Exception('invalid image name')

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

def main(args):
    if args.workdir:
        tmp_dir = args.workdir
    else:
        tmp_dir = '/tmp/.transport_docker_image/' + rand_str()

    source_ssh_client, source_image_name = parse_image_name(args.source_image)
    target_ssh_client, target_image_name = parse_image_name(args.target_image)

    if source_ssh_client is None and target_ssh_client is None:
        raise Exception('at least one end should be using ssh')

    if args.pre_hook:
        exec_command(args.pre_hook, ssh_client=target_ssh_client, print_stdout=True, print_stderr=True)

    exec_command('mkdir -p %s' % shlex.quote(os.path.join(tmp_dir, source_image_name)), ssh_client=source_ssh_client)
    exec_command('%s save -o %s %s' % (
        args.source_docker_path,
        shlex.quote(os.path.join(tmp_dir, source_image_name + '.tar')),
        shlex.quote(source_image_name),
    ), ssh_client=source_ssh_client)
    exec_command('tar -x -f %s -C %s' % (
        shlex.quote(os.path.join(tmp_dir, source_image_name + '.tar')),
        shlex.quote(os.path.join(tmp_dir, source_image_name)),
    ), ssh_client=source_ssh_client)

    stdout, stderr = exec_command('%s inspect %s --format "{{json .RootFS.Layers}}"' % (args.target_docker_path, shlex.quote(target_image_name)), ssh_client=target_ssh_client, print_stderr=True)

    if (not stdout or not stdout.strip()) and b'Error: No such object:' in stderr:
        logger.info('target image not found at destination, could not shrink size')
    else:
        existing_layers = json.loads(stdout)
        assert isinstance(existing_layers, list)

        clean_file = string.Template(clean_template).substitute(existing_layers=json.dumps(existing_layers), image_name=target_image_name)
        write_file(os.path.join(tmp_dir, source_image_name, 'clean.py'), clean_file.encode(), ssh_client=source_ssh_client)

        exec_command('python %s' % (
            shlex.quote(os.path.join(tmp_dir, source_image_name, 'clean.py'))
        ), ssh_client=source_ssh_client, print_stderr=True)

        exec_command('rm -f %s' % (
            shlex.quote(os.path.join(tmp_dir, source_image_name, 'clean.py'))
        ), ssh_client=source_ssh_client, print_stderr=True)

    shrinked_path = os.path.join(tmp_dir, source_image_name + '.shrinked.tar.gz')

    exec_command('tar -c -z -f %s -C %s .' % (
        shlex.quote(shrinked_path),
        shlex.quote(os.path.join(tmp_dir, source_image_name))
    ), ssh_client=source_ssh_client, print_stderr=True)

    exec_command('mkdir -p %s' % shlex.quote(os.path.join(tmp_dir, source_image_name)), ssh_client=target_ssh_client)

    size = file_size(shrinked_path, ssh_client=source_ssh_client)
    reader = open_file(shrinked_path, mode='rb', ssh_client=source_ssh_client)
    writer = open_file(shrinked_path, mode='wb', ssh_client=target_ssh_client)

    transfered = 0
    transfer_begin_time = time.time()
    print('transfer started...', file=sys.stderr)
    while True:
        content = reader.read(64 * 1024)
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
            shlex.quote(os.path.join(tmp_dir, source_image_name + '.tar')),
            shlex.quote(os.path.join(tmp_dir, source_image_name)),
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

if __name__ == '__main__':
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

    args = parser.parse_args()

    logging.basicConfig(level=args.loglevel)
    main(args)
