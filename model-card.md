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

_Basic information about the model._

Review section 4.1 of the [model cards paper](https://arxiv.org/abs/1810.03993).

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

_Use cases that were envisioned during development._

Review section 4.2 of the [model cards paper](https://arxiv.org/abs/1810.03993).

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

_Factors could include demographic or phenotypic groups, environmental conditions, technical
attributes, or others listed in Section 4.3._

Review section 4.3 of the [model cards paper](https://arxiv.org/abs/1810.03993).

### Relevant factors

Anomaly type, frequency, duration, and severity.

## Metrics

_The appropriate metrics to feature in a model card depend on the type of model that is being tested.
For example, classification systems in which the primary output is a class label differ significantly
from systems whose primary output is a score. In all cases, the reported metrics should be determined
based on the model’s structure and intended use._

Review section 4.4 of the [model cards paper](https://arxiv.org/abs/1810.03993).

### Model performance measures

Precision and recall, AUC-ROC, and user satisfaction.

### Decision thresholds

Thresholds are provided as hyperparameters; can be set by users.

## Evaluation data

_All referenced datasets would ideally point to any set of documents that provide visibility into the
source and composition of the dataset. Evaluation datasets should include datasets that are publicly
available for third-party use. These could be existing datasets or new ones provided alongside the model
card analyses to enable further benchmarking._

Review section 4.5 of the [model cards paper](https://arxiv.org/abs/1810.03993).

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

Review section 4.6 of the [model cards paper](https://arxiv.org/abs/1810.03993).

## Quantitative analyses

_Quantitative analyses should be disaggregated, that is, broken down by the chosen factors. Quantitative
analyses should provide the results of evaluating the model according to the chosen metrics, providing
confidence interval values when possible._

Review section 4.7 of the [model cards paper](https://arxiv.org/abs/1810.03993).

### Unitary results

To be determined.

### Intersectional result

To be determined.

## Ethical considerations

_This section is intended to demonstrate the ethical considerations that went into model development,
surfacing ethical challenges and solutions to stakeholders. Ethical analysis does not always lead to
precise solutions, but the process of ethical contemplation is worthwhile to inform on responsible
practices and next steps in future work._

Review section 4.8 of the [model cards paper](https://arxiv.org/abs/1810.03993).

### Data

GLADoSS makes no assumptions about the content of the data it is presented
with. While envisioned for the smart building domain, data producers (e.g.,
devices) might provide sensitive data.

### Risks and harms

GLADoSS aims to highlight anomalies in the data. Decision made or actions taken
based on an imprecise interpretation of the reported anomalies may occur.

## Caveats and recommendations

_This section should list additional concerns that were not covered in the previous sections._

Review section 4.9 of the [model cards paper](https://arxiv.org/abs/1810.03993).

None identified.
