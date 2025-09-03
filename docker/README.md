# GLADoSS Docker Image

The latest version of GLADoSS can be deployed via Docker, by building and then running an image using the files provided in this directory. Hereto, clone or download these files to a local directory and execute the following command:

    # docker-compose up gladoss

The above command will build the image (if not already done so before) and start the application as a service. The `entrypoint.sh` file can be edited to customise the parameters with which the application will be run, whereas custom backup and logging locations can be set in the `compose.yaml` file.

The following command can be used to stop the container:

    # docker-compose down gladoss

When using the provided compose file the running containers are connected via a dedicated Docker network named `semantic_network`. Only messages sent via this network are visible to the containers. To allow for communication between arbitrary devices, add the devices to this network (if dockerized) or edit the network settings in the compose file to use a different network (e.g. that of the host). 

Note that the image will have to be rebuild each time a custom adaptor is added.

## Demo Mode

The application can be run in demo mode, in which it will simulate a smart device that streams RDF data via a REST API. By running the application in both normal and demo mode (via two running containers) the application can be tested with simulated data in a controlled environment.

The following command starts the application in demo mode:

    # docker-compose up gladoss-demo

The following command can be used to stop the container:

    # docker-compose down gladoss-demo

The parameters of the application in demo mode can be customised by editing the `compose.yaml` file.
