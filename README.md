# GLADoSS - Graph-based Live Anomaly Detection on Semantic Streams

**THIS IS A WORK IN PROGRESS**

GLADoSS is a Python program designed for the real-time detection of (possible future) anomalies in a series of state graphs in distributed environments. Each state graph represents the self-reported state of a software agent (e.g., a smart device) at a certain point in time, which has been published to a network by that agent itself or an information broker (e.g., a [Knowledge Engine](https://github.com/TNO/knowledge-engine). For each agent, GLADoSS will attempt to learn patterns of nominal behaviour by analysing the structure and data of the received graphs, as well as their semantics. Symbolic and subsymbolic methods are employed to discover deviations from the learned nominal behaviour which, when deemed significant, are highlighted and reported upon, and accompanied by possible explanations. The reports can be published to the same network (as [SHACL](https://www.w3.org/TR/shacl/) validation report) enabling dashboards and other interfaces to receive and display the information. By applying continuous online learning, GLADoSS is able to maintain an up-to-date pattern of nominal behaviour for each monitored agent, as well as adapt to natural shifts in behaviour.

## Data Requirements

Both incoming state information and outgoing reports are expected to be encoded as well-formed graphs conforming to the [_Resource Description Framework_ (RDF)](https://www.w3.org/TR/rdf11-concepts/). RDF is an open [W3C](https://www.w3.org/) standard in which knowledge, information, and data are encoded using binary statements, often called triples. Triples relate an object `o` to its subject `s` via a relationship `p`, and are represented via the infix notation: `s, p, o`. Here, both subject `s` and relationship `p` are denoted via _Universal Resource Identifiers_ (URI) which are a generalisation of the URL, whereas object `o` can be either a URI or a raw value, called a literal.

The following graph is an example of the expected input data, and is encoded using the [Turtle serialization format](https://www.w3.org/TR/turtle/):

```Turtle
@PREFIX rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@PREFIX saref: <http://saref.etsi.org/core/> .
@PREFIX xsd:   <http://www.w3.org/2001/XMLSchema#> .
@PREFIX ex:    <http://example.org/demo/> .

ex:Uwmi0dxkgf8qjrd9bmsjc rdf:type saref:Observation .
ex:Uwmi0dxkgf8qjrd9bmsjc saref:madeBy ex:Uqwodyj51zp144lnzuqtw> .
ex:Uwmi0dxkgf8qjrd9bmsjc saref:hasTimestamp "2024-12-30T16:55:33.257238"^^xsd:dateTime .
ex:Uwmi0dxkgf8qjrd9bmsjc saref:hasResult ex:Uwm2wgp669iloqrcjz8j7> .

ex:Uwm2wgp669iloqrcjz8j7 rdf:type saref:PropertyValue .
ex:Uwm2wgp669iloqrcjz8j7 saref:isValueOfProperty ex:Energy .
ex:Uwm2wgp669iloqrcjz8j7 saref:hasValue "-1.93"^^xsd:float .
ex:Uwm2wgp669iloqrcjz8j7 saref:isMeasuredIn "kWh"^^xsd:string .
```

The following graph is an example of the expected output data, and is encoded using the [Turtle serialization format](https://www.w3.org/TR/turtle/):

```Turtle
@PREFIX rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@PREFIX rdfs:  <http://www.w3.org/2000/01/rdf-schema#> .
@PREFIX sh:    <http://www.w3.org/ns/shacl#> .
@PREFIX xsd:   <http://www.w3.org/2001/XMLSchema#> .
@PREFIX dct:   <http://purl.org/dc/terms/> .
@PREFIX saref: <http://saref.etsi.org/core/> .
@PREFIX ex:    <http://example.org/demo/> .

_:BUtcw1hbu2x53tsalyktei rdf:type sh:ValidationReport .
_:BUtcw1hbu2x53tsalyktei dct:date "2025-07-07T12:12:45.482481"^^xsd:dateTime .
_:BUtcw1hbu2x53tsalyktei dct:identifier ex:Uwmi0dxkgf8qjrd9bmsjc .
_:BUtcw1hbu2x53tsalyktei dct:conformsTo "https://www.w3.org/TR/shacl/"^^xsd:anyURI .
_:BUtcw1hbu2x53tsalyktei dct:hasPart _:BUifsw7dl0q491dz9z21i6 .
_:BUtcw1hbu2x53tsalyktei sh:conforms "false"^^xsd:boolean .

_:BUifsw7dl0q491dz9z21i6 rdf:type sh:ValidationResult .
_:BUifsw7dl0q491dz9z21i6 sh:focusNode ex:Uwm2wgp669iloqrcjz8j7 .
_:BUifsw7dl0q491dz9z21i6 sh:resultPath saref:hasValue .
_:BUifsw7dl0q491dz9z21i6 sh:value "47.09"^^xsd:float
_:BUifsw7dl0q491dz9z21i6 sh:sourceShape _:Ugknckoyxclhxua61qy7m .
_:BUifsw7dl0q491dz9z21i6 rdfs:label "Critical Value Violation"@en .
_:BUifsw7dl0q491dz9z21i6 sh:resultMessage "Evidence from the statistical evaluation suggests that this triple of the observed state graph differs significantly from the associated graph pattern at the critical level (alpha = 0.05)."@en
_:BUifsw7dl0q491dz9z21i6 sh:resultSeverity _:BU12ymsmziok9c29pjtt9i .

_:BU12ymsmziok9c29pjtt9i rdf:type sh:Severity .
_:BU12ymsmziok9c29pjtt9i rdfs:label "CRITICAL"@en .
_:BU12ymsmziok9c29pjtt9i rdfs:comment "Critical Anomaly"@en .
```

## Installation

GLADoSS can be installed using [PIP](https://pip.pypa.io/en/stable/), the package installer for Python. Instructions on how to do so are given next. These instructions assume that the system has working and updated [Python](https://www.python.org/) and [Git](https://git-scm.com/) installations. *It is strongly recommended to first set up a clean [virtual environment](https://packaging.python.org/en/latest/guides/installing-using-pip-and-virtual-environments/#create-and-use-virtual-environments) before continuing with installation (see next header).*

To install GLADoSS,

1) Clone this repository to your device

```
$ git clone https://gitlab.com/wxwilcke/gladoss.git
```

2) Change the current directory to the root of the repository code

```
$ cd gladoss/
```

3) Install via PIP

```
$ pip install .
```

### Python Virtual Environment

A [Python virtual environment](https://packaging.python.org/en/latest/guides/installing-using-pip-and-virtual-environments/#create-and-use-virtual-environments) provides a clean and isolated environment that minimizes issues with dependencies and other packages. It is strongly recommended to install GLADoSS in a virtual environment, as well as to run it from there. Instructions on how to set up such an environment are given next.

1) Create a Python virtual environment in the current directory. Replace `<name>` by the name you want this environment to be called.

```
$ python -m venv <name>
```

2) Activate the environment. This step has to be repeated each time you want to run GLADoSS.

    a) for Windows

```
$ <name>\Scripts\activate
```


    b) for Mac / Linux

```
$ source <name>/bin/activate
```

The Python virtual environment is now set up and activated. Any calls to Python or PIP will now use this environment. GLADoSS can now be installed in the activated virtual environment by executing the three steps listed above. Once installed, GLADoSS can also be run.

3) Deactivate the environment. Ensure that GLADoSS is no longer running.

    a) for Windows

```
$ <name>\Scripts\deactivate
```


    b) for Mac / Linux

```
$ source <name>/bin/deactivate
```

## Demo

The GLADoSS repository contains a simple stand-alone demo which simulates an IoT network and device. Instructions on how to start the demo are given next. Note that this requires two terminals (both with activated environments) or a multiplexer like `tmux`.

1) (Optional) Active the virtual environment. Replace `<name>` with the name of the actual environment.

    a) for Windows

```
$ <name>\Scripts\activate
```


    b) for Mac / Linux

```
$ source <name>/bin/activate
```

2) Change the current directory to the root of the repository code

```
$ cd gladoss/
```

3) Start the simulated smart device(s) in one terminal (A).

```
$ python gladoss/demo/dummy-device.py -v --no-autocycle -i gladoss/demo/dummy-data.json
```

4) Start GLADoSS in another terminal (B)

```
$ python gladoss/run.py -v --grace_period=10 --report_level=0 dummy
```

5) Run the demo

    a) Select terminal A and press any key to simulate the publication of a state

    b) Watch the result on terminal B. Reports will be published after the second encounter of a state from the same device, and validation will be skipped for the first ten encounters.

6) Terminate the simulator and GLADoSS by pressing *CTRL-C* several times.

---

This project is funded by the Hedge-IoT project
