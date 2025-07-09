
# transport-docker-image

`transport-docker-image` provides a simple way to transport docker images with best efforts to reduce transmission size by removing existing layers in target repo.

## Installation

```
$ pip install transport-docker-image
```

# Example Usage

podman also support

```
$ transport-docker-image --workdir /home/ztx/mytmp/ --source-docker-path /usr/bin/podman --target-docker-path /usr/bin/podman root@10.4.4.2:/imagename:8.0 imagename:8.0
```

specify chunk size

```
$ transport-docker-image --chunk-size 1024 $src $dst
```
