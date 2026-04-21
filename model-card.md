# Model card for GLADoSS

Sections and prompts from the [model cards paper](https://arxiv.org/abs/1810.03993), v2.

Jump to section:

- [Model details](#model-details)
- [Intended use](#intended-use)
- [Factors](#factors)
- [Metrics](#metrics)
- [Evaluation data](#evaluation-data)
- [Training data](#training-data)
- [Quantitative analyses](#quantitative-analyses)
- [Ethical considerations](#ethical-considerations)
- [Caveats and recommendations](#caveats-and-recommendations)

## Model details

- Person or organization developing model

    W.X. Wilcke (Vrije Universiteit Amsterdam) - w.x.wilcke[at]vu.nl

- Model date

    2026

- Model version

    Development version, status Alpha

- Model type

   (sub-) Symbolic model for continuous online learning on data streams 

- Paper or other resource for more information

    In process.

- Citation details

    Wilcke,W.X. & Ronald R.M. & de Boer V., "GLADoSS - Graph-based Live Anomaly
    Detection on Semantic Streams", 2026, https://gitlab.com/wxwilcke/gladoss

- License

    GPLv3

- Where to send questions or comments about the model

    w.x.wilcke[at]vu.nl

## Intended use

### Primary intended uses

GLADoSS is designed for the real-time detection of (possible future) anomalies
in a series of state graphs in distributed environments. Each state graph
represents the self-reported state of a software agent (e.g., a smart device)
at a certain point in time, which has been published to a network by that agent
itself or an information broker. GLADoSS will attempt to learn patterns of
nominal behaviour by analysing the structure and data of the received graphs,
as well as their semantics. Symbolic and subsymbolic methods are employed to
discover deviations from the learned nominal behaviour which, when deemed
significant, are highlighted and reported upon, and accompanied by possible
explanations. The reports can be published to the same network.

### Primary intended users

GLADoSS primarily targets home and building owners and operators of smart
environments.

### Out-of-scope use cases

GLADoSS is agnostic to the domain of the data and can therefore be used for any
use case that involves anomaly detection in semantic data streams. When streams
can be simulated, the use case can be generalised to graph anomaly detection.

## Factors

### Relevant factors

Anomaly type, frequency, duration, and severity.

## Metrics

### Model performance measures

Precision and recall, AUC-ROC, and user satisfaction.

### Decision thresholds

Thresholds are provided as hyperparameters; can be set by users.

## Evaluation data

### Datasets

- ApartmentGraph
- OfficeGraph
- CampusGraph
- Synthetic data

### Motivation

All datasets hold state information of a multitude of smart devices over a
period of time, and therefore fit the intended use well. Three datasets contain
real-world data whereas synthetic data is used for controlled experiments.

### Preprocessing

Data should be provided as streams of RDF-encoded, timestamped graphs.

## Training data

GLADoSS applies online, continuous learning to construct patterns of nominal
device behaviour. All device data presented during the grace period can be
considered as training data.

## Quantitative analyses

### Unitary results

To be determined.

### Intersectional result

To be determined.

## Ethical considerations

### Data

GLADoSS makes no assumptions about the content of the data it is presented
with. While envisioned for the smart building domain, data producers (e.g.,
devices) might provide sensitive data.

### Risks and harms

GLADoSS aims to highlight anomalies in the data. Decision made or actions taken
based on an imprecise interpretation of the reported anomalies may occur.
Learned patterns are subject to aggregation operations and not pertain to
individual data points (e.g., a person).

## Caveats and recommendations

None identified.
