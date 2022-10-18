
# Examples

podman also support

```
$ transport_docker_image.py --workdir /home/ztx/mytmp/ --source-docker-path /usr/bin/podman --target-docker-path /usr/bin/podman ztx@10.4.4.2:/some:8.0 some:8.0
```

specify chunk size

```
$ transport_docker_image.py --chunk-size 1024 $src $dst
```
