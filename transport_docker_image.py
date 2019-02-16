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
from urllib.parse import urlparse

import paramiko

logger = logging.getLogger('TransDockerImage')

'''
docker history bmitch3020/terraform-ansible -q | while read image_id; do
    echo "$image_id"; \
    if [ "$image_id" != "<missing>" ]; then \
        docker inspect "$image_id" --format '{{json .RootFS.Layers}}' | jq .; \
    fi; \
done
'''

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
    logger.info('[ STEP ] %s' % command)
    if ssh_client is not None:
        stdin_io, stdout_io, stderr_io = ssh_client.exec_command(command)
        stdout = stdout_io.read()
        if print_stdout:
            print(stdout, file=sys.stderr)
        if print_stderr:
            print(stderr_io.read(), file=sys.stderr)
        return stdout
    else:
        cmd = shlex.split(command)
        kwargs = {
            'encoding': 'utf-8',
        }
        if print_stderr:
            kwargs['stderr'] = subprocess.PIPE
        proc = subprocess.run(cmd, **kwargs)
        if print_stdout:
            print(proc.stdout, file=sys.stderr)
        if print_stderr:
            print(proc.stderr, file=sys.stderr)
        return proc.stdout

def main(args):
    if args.workdir:
        tmp_dir = args.workdir
    else:
        tmp_dir = '/tmp/.transport_docker_image/' + rand_str()

    source_ssh_client, source_image_name = parse_image_name(args.source_image)
    target_ssh_client, target_image_name = parse_image_name(args.target_image)

    if source_ssh_client is None and target_ssh_client is None:
        raise Exception('at least one end should be using ssh')

    exec_command('mkdir -p %s' % shlex.quote(os.path.join(tmp_dir, source_image_name)), ssh_client=source_ssh_client)
    exec_command('docker save -o %s %s' % (
        shlex.quote(os.path.join(tmp_dir, source_image_name + '.tar')),
        shlex.quote(source_image_name),
    ), ssh_client=source_ssh_client)
    exec_command('tar -x -f %s -C %s' % (
        shlex.quote(os.path.join(tmp_dir, source_image_name + '.tar')),
        shlex.quote(os.path.join(tmp_dir, source_image_name)),
    ), ssh_client=source_ssh_client)

    output = exec_command('docker inspect %s --format "{{json .RootFS.Layers}}"' % shlex.quote(target_image_name), ssh_client=target_ssh_client)

    existing_layers = json.loads(output)
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
    while True:
        content = reader.read(64 * 1024)
        if not content:
            break
        writer.write(content)
        transfered += len(content)
        print('transfered %d/%d' % (transfered, size), file=sys.stderr)

    reader.close()
    writer.close()

    exec_command('docker load -i %s' % (
        shlex.quote(shrinked_path),
    ), ssh_client=target_ssh_client, print_stdout=True, print_stderr=True)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Transport docker image with best effort to reduce transmission size')
    parser.add_argument('source_image', help='Source image in format: [user@host[:port]/]image:tag')
    parser.add_argument('target_image', help='Target image in format: [user@host[:port]/]image:tag')
    parser.add_argument('--workdir', help='Specify workdir for tempfile, defaults to random dir under /tmp', required=False)
    parser.add_argument('--loglevel', help='Specify log level', required=False, default=logging.INFO)

    args = parser.parse_args()

    logging.basicConfig(level=args.loglevel)
    main(args)
