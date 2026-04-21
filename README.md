# GLADoSS - Graph-based Live Anomaly Detection on Semantic Streams

- [Official Repository (GitLab)](https://gitlab.com/wxwilcke/gladoss)
- [Mirror Repository (GitHub)](https://github.com/wxwilcke/gladoss)

- [Model Card](model-card.md)

**THIS IS A WORK IN PROGRESS**

GLADoSS is a Python program designed for the real-time detection of (possible future) anomalies in a series of state graphs in distributed environments. Each state graph represents the self-reported state of a software agent (e.g., a smart device) at a certain point in time, which has been published to a network by that agent itself or an information broker (e.g., a [Knowledge Engine](https://github.com/TNO/knowledge-engine)). For each agent, GLADoSS will attempt to learn patterns of nominal behaviour by analysing the structure and data of the received graphs, as well as their semantics. Symbolic and subsymbolic methods are employed to discover deviations from the learned nominal behaviour which, when deemed significant, are highlighted and reported upon, and accompanied by possible explanations. The reports can be published to the same network (as [SHACL](https://www.w3.org/TR/shacl/) validation report) enabling dashboards and other interfaces to receive and display the information. By applying continuous online learning, GLADoSS is able to maintain an up-to-date pattern of nominal behaviour for each monitored agent, as well as adapt to natural shifts in behaviour.

## Data Requirements

Both incoming state information and outgoing reports are expected to be encoded as well-formed graphs conforming to the [_Resource Description Framework_ (RDF)](https://www.w3.org/TR/rdf11-concepts/). RDF is an open [W3C](https://www.w3.org/) standard in which knowledge, information, and data are encoded using binary statements, often called triples. Triples relate an object `o` to its subject `s` via a relationship `p`, and are represented via the infix notation: `s, p, o`. Here, both subject `s` and relationship `p` are denoted via _Universal Resource Identifiers_ (URI) which are a generalisation of the URL, whereas object `o` can be either a URI or a raw value, called a literal. Literals can have an optional datatype annotation, provided as URI, or language tag.

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
_:BUifsw7dl0q491dz9z21i6 rdfs:label "Critical Value Violation"@en .
_:BUifsw7dl0q491dz9z21i6 sh:resultMessage "Evidence from the statistical evaluation suggests that this triple of the observed state graph differs significantly from the associated graph pattern at the critical level (alpha = 0.05)."@en
_:BUifsw7dl0q491dz9z21i6 sh:resultSeverity _:BU12ymsmziok9c29pjtt9i .

_:BU12ymsmziok9c29pjtt9i rdf:type sh:Severity .
_:BU12ymsmziok9c29pjtt9i rdfs:label "CRITICAL"@en .
_:BU12ymsmziok9c29pjtt9i rdfs:comment "Critical Anomaly"@en .
```

## Anomalies

The following anomalies can be detected by GLADoSS:

- Structural Violations¹
    - Exact Match Requirement Violation:
        - Number of observed assertions is higher than expected.
        - Closed-World Assumption Violation:
            - Number of observed assertions is lower than expected. 
- Semantic Violations
    - Resource Type Violation:
        - Observed resource type differs from expected resource type.
    - Object Type Violation:
        - Observed object type differs from expected object type.
    - Data Type Violation:
        - Observed Literal value data type differs from expected literal value data type.
    - Data Language Violation:
        - Observed literal value language differs from expected literal value language.
    - Predicate Equality Violation:
        - Observed predicate at this position differs from expected predicate.
- Value Violations
    - Value Equality Violation:
        - Observed IRI value differs from expected IRI value.
        - Observed Literal value differs from expected literal value.
    - Critical Value Violation²:
        - Observed Literal value differs significantly at the critical level.
    - Suspicious Value Violation²:
        - Observed Literal value differs significantly at the suspicious level.

1. Structure validation is disabled by default
2. Only relevant when validating distributions of dynamic assertions

## Run

GLADoSS can be run via the following command:

    $ python -m gladoss [OPTIONS] <ADAPTOR>

with a mandatory adaptor (`ADAPTOR`) and zero of more optional options (`OPTIONS`). Us the help flag (`--help`) to view all possible options:

    $ python -m gladoss -h

## Installation

GLADoSS can be installed using [PIP](https://pip.pypa.io/en/stable/), the package installer for Python. Instructions on how to do so are given next. These instructions assume that the system has working and updated [Python](https://www.python.org/) and [Git](https://git-scm.com/) installations. *It is strongly recommended to first set up a clean [virtual environment](https://packaging.python.org/en/latest/guides/installing-using-pip-and-virtual-environments/#create-and-use-virtual-environments) before continuing with installation (see next header).*

To install GLADoSS

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

        $ python -m venv <name>

2) Activate the environment. This step has to be repeated each time you want to run GLADoSS.

    a) for Windows

        $ <name>\Scripts\activate

    b) for Mac / Linux

        $ source <name>/bin/activate

The Python virtual environment is now set up and activated. Any calls to Python or PIP will now use this environment. GLADoSS can now be installed in the activated virtual environment by executing the three steps listed above. Once installed, GLADoSS can also be run.

3) Deactivate the environment. Ensure that GLADoSS is no longer running.

    a) for Windows

        $ <name>\Scripts\deactivate

    b) for Mac / Linux

        $ source <name>/bin/deactivate

## Demo

The GLADoSS repository contains a simple stand-alone demo which simulates an IoT network and device. Instructions on how to start the demo are given next. Note that this requires two terminals (both with activated environments) or a multiplexer like `tmux`.

1) (Optional) Active the virtual environment. Replace `<name>` with the name of the actual environment.

    a) for Windows

        $ <name>\Scripts\activate

    b) for Mac / Linux

        $ source <name>/bin/activate

2) Start the simulated smart device(s) in one terminal (A).

        $ python -m gladoss demo -v --no-autocycle -i gladoss/demo/dummy-data.json

3) Start GLADoSS in another terminal (B)

        $ python -m gladoss -v --grace-period=10 --report-level=0 demo

4) Run the demo

    a) Select terminal A and press any key to simulate the publication of a state

    b) Watch the result on terminal B. Reports will be published after the second encounter of a state from the same device, and validation will be skipped for the first ten encounters.

6) Terminate the simulator and GLADoSS by pressing *CTRL-C* several times.

The demo is also available as Docker container (see `docker/`)

## Custom Adaptor

Adaptors form the bridge between GLADoSS and the various knowledge producers or relays, handling incoming and outgoing communication as well as the translation between various data formats. Custom adaptors can be used to enable GLADoSS to work in different environments and with different forms of data.

To create a custom adaptor

1) Create a new Python file in a directory of choice, for example `adaptors/custom_adaptor.py`.

2) Create a new class in the just-created file. This class must be a subclass of the abstract base class `Adaptor`.

3) Write the necessary procedures. These procedures are defined in the abstract base class, and include

    - procedures to set the headers and body of the various HTTP requests
    - procedures to translate the incoming data to N-Triples format
    - procedures to publish (and optionaly translate) the validation report

In addition, a procedure has to be written that creates one on more connections, one per endpoint. If necessary, an initialisation and clean-up hook can also be used that will run before and after establishing the connection, and a context dictionary can be used to share data between procedures.

4) Set the environment variable `GLADOSS_ADAPTOR_DIRECTORY` to point to the directory that contains your adaptors, and tell GLADoSS to use your custom adaptor:

```bash
env GLADOSS_ADAPTOR_DIRECTORY=${PWD}/adaptors/ python -m gladoss -v custom_adaptor
```
---

This project is funded by the Hedge-IoT project
