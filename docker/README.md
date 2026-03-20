# GLADoSS Docker Image

The latest version of GLADoSS can be deployed via Docker by building and then running an image using the files provided in this directory. Hereto, clone or download these files to a local directory and follow the instruction below.

1) Provide the appropriate adaptor in the entrypoint file. By default this is set to use the `demo` adaptor.

```bash
gladoss-run --verbose --continuous demo
```

The `continuous` flag tells GLADoSS to keep listening for incoming messages regardless of communication and/or data issues.

2) Build a fresh container image with the aforementioned entrypoint file..

```bash
docker build --build-context base=.. \
             --build-arg UID=$(id -u) \
             --build-arg GID=$(id -g) \
             -f Dockerfile \
             -t gladoss .
```

This build uses the default `Dockerfile` which assumes that the source code is
locally available (e.g. following `git clone`). To use upstream instead,
replace `Dockerfile` in the build command by `git.Dockerfile`.

3) Run the GLADoSS container on the `semantic_network` and with local directories for backups and (custom) adaptors accessible from the container.

```bash
docker run --network semantic_network \
           --name gladoss \
           --mount src=./backup/,target=/mnt/backup,type=bind \
           --mount src=./adaptors/,target=/etc/gladoss/adaptors,type=bind \
           gladoss
```

Alternatively, Docker compose can be used to manage the container:

```bash
docker-compose up -d gladoss
```

The above commands will build the image and start the application as a service. The `entrypoint.sh` file can be edited to customise the parameters with which the application will be run, whereas custom backup and (custom) adaptor locations can be set in the `compose.yaml` file.

Once the container is running the following command can be used to view the log output:

```bash
docker logs -f gladoss
```

The following command can be used to stop the container:

```bash
docker stop gladoss
```

or when Docker compose is used:

```bash
docker-compose down gladoss
```

When using the above commands or the provided compose file the running containers are connected via a dedicated Docker network named `semantic_network`. Only messages sent via this network are visible to the containers. To allow for communication between arbitrary devices, add the devices to this network (if dockerized) or edit the network settings in the compose file to use a different network (e.g. that of the host). 

Note that the image will have to be rebuild each time a custom adaptor is added.

## Demo Mode

The application can be run in demo mode, in which it will simulate a smart device that streams RDF data via a REST API. By running the application in both normal and demo mode (via two running containers) the application can be tested with simulated data in a controlled environment.

The following command starts the application in demo mode:

    # docker-compose up -d gladoss-demo

The following command can be used to stop the container:

    # docker-compose down gladoss-demo

The parameters of the application in demo mode can be customised by editing the `compose.yaml` file.
