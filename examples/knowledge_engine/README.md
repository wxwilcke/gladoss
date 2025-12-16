# Integration with Knowledge Engine

The [Knowledge Engine](https://knowledge-engine.eu) is a service that
facilitates smart data exchange between two or more devices using
Semantic Web Technologies. For this purpose, the Knowledge Engine
acts as an information broker that is build upon a *pub/sub* model:
publishers post data to the Knowledge Engine which relays them to
relevant subscribers. Both publishers and subscribers must register
themselves and their intent at the Knowledge Engine before data can
be send or received. Multiple (possibly collaborating) Knowledge
Engine instances can be active at any moment.

GLADoSS can be used to monitor the data streams between devices and
report on possible anomalies. This can be achieved by letting GLADoSS
become a node in the data exchange network by registering it with one
or more Knowledge Engine instances and by subscribing to one or more
publishing devices. GLADoSS will then receive live data streams from
relevant devices via the associated Knowledge Engine instances. By
also registering GLADoSS as a publisher, anomaly reports can be send
back to the data exchange network via the same Knowledge Engine
instances. Devices subscribed to these reports (e.g., dashboards) can
then receive and display their content.

## Adaptor

An appropriate adaptor is needed to facilitate the communication with
the Knowledge Engine. An implementation of such an adaptor can be found
in the `adaptors/` directory.

The adaptor must be configured to specify with which Knowledge Engine
instance GLADoSS must be registered and to which messages GLADoSS must
be subscribed. The configuration can be set in the accompanied `toml`
file. Multiple subscriptions to multiple Knowledge Engine instances
can be set.

A basic example is shown below.

``` toml
knowledgeBaseId = "http://example.org/gladoss"
knowledgeBaseName = "GLADoSS"
knowledgeBaseDescription = "Graph-based Live Anomaly Detection on Semantic Streams"

[[knowledgeInteraction]]
knowledgeInteractionEndpoint = "http://127.0.0.1:8280/rest"
knowledgeInteractionName = "DeviceObservationsRequest"
argumentGraphPattern = """
  ?sensor rdf:type saref:Sensor .
  ?measurement saref:measurementMadeBy ?sensor .
  ?measurement saref:isMeasuredIn saref:TemperatureUnit .
  ?measurement saref:hasValue ?temperature .
  ?measurement saref:hasTimestamp ?timestamp .
  """
[knowledgeInteraction.prefixes]
rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
saref = "https://saref.etsi.org/core/"
```

The above configuration defines GLADoSS as a knowledge base in the
data exchange network (line 1 -- 3) and specifies a single subscription
(called *DeviceObservationsRequest*) with a single Knowledge Base
instance (at *http://127.0.0.1:8280*). Messages send to this instance
that match the subscribed graph pattern will be forwarded to GLADoSS.

The report publication registration occurs automatically; no specific
configuration is necessary.

## Running with Docker

Running GLADoSS together with the Knowledge Engine using Docker requires
appropriate adaptors (see above) and that both containers run on the same
network and can see each other. The steps below provide a working example
setup.

1) Run the Knowledge Engine container with an appropriate name and network:

```bash
docker run --network semantic_network \
           --name knowledge-engine \
           --port 8280:8280 \
           ghcr.io/tno/knowledge-engine/smart-connector:1.4.0
```

2) Set the correct endpoint in the adaptor configuration file:

```toml
[...]
knowledgeInteractionEndpoint = "http://knowledge-engine:8280/rest"
[...]
```

3) Set the appropriate adaptor in the entrypoint file:

```bash
gladoss-run --verbose --continuous knowledge_engine
```

4) Build a fresh container image with the aforementioned entrypoint file:

```bash
docker build --build-arg UID=$(id -u) --build-arg GID=$(id -g) -t gladoss-ke .
```

5) Run the GLADoSS container with the same network as above and with (at least)
the adaptor directory mounted at the appropriate mount point in the container
(`/etc/gladoss/adaptors/`):

```bash
docker run --network semantic_network \
           --name gladoss-ke \
           --mount src=./backup/,target=/mnt/backup,type=bind \
           --mount src=./logs/,target=/var/log/gladoss,type=bind \
           --mount src=./adaptors/,target=/etc/gladoss/adaptors,type=bind \
           gladoss-ke
```

Alternatively, Docker compose can be used to manage the container.
