# UIMA JCas Repository Service
A scalable UIMA Cas/JCas checkpointing service.

## Motivation

The Unstructured Information Management Architecture (UIMA) is an open source framework that focuses on the building of performant NLP processing pipelines. In the words of the Apache UIMA project:

> Unstructured Information Management applications are software systems that analyze large volumes of unstructured information in order to discover knowledge that is relevant to an end user.

Two major components of UIMA are the Collection Readers (CR) and Analysis Engines (AE). The AEs are the muscle of UIMA. They are the components that analyze, extract, and annotate unstructured text. They operate on JCases. A JCas is the java-version of a Cas and can be thought of as a data object representing a document, or a single unit of analysis. AEs can be combined to form complex processing pipelines. These pipelines can be run locally as a Collection Processing Engine (CPE) or it can be persisted and scaled out with UIMA AS/UIMA DUCC. CRs are used to turn text from some source (e.g., file system) into JCases and sent to a CPE or scaled-out pipeline.

One of the problems with UIMA's processing methodology is the lack of checkpointing. A JCas can only be persisted once it has been processed by by the pipeline. In other words, unless there is an AE in a pipeline, a JCas only exists in memory from the time it is created by the CR until it has gone through the CPE or scaled-out pipeline. That means that if there is a failure in the pipeline, the document the pipeline was processing has to be processed all over again.

Now, there are several approaches that can be taken to solve this. A JCas can be persisted after each AE in a pipeline. Therefore, a JCas can be checkpointed at the AE level. However, the design of UIMA favors the creation and use of fairly simple, well-defined, self-contained AES that can be combined to do more complex processing. Therefore, a pipeline may consist of many AEs strung together in sequence. That would mean that the overhead associated with persistence is incurred often if the JCas is persisted after each AE. Another approach is to persist the JCas after several AEs, reducing the incurred persistence cost.

## A Solution

The UIMA JCas/Cas Repository Service (CRS) takes the latter approach. Instead of defining a set number of AEs before checkpointing, the CRS allows this number to be variable and set by the user. Checkpointing is done with AEs that are configured to communicate with the CRS. These AEs can be added to the pipeline where, and in whatever number, is desired. Checkpoint restoration is done by using CRs that communicate with the CRS and its metadata store. These CRs and AEs are created by the user to meet their needs.

Effectively, the AEs and CRs mean that a pipeline is broken up into smaller pipelines and glued together. In addition, the CRS allows the for the caller to designate where a CAS should be registered in the metadata store. By limiting each CRS-aware CR to a portion of the metadata store, a sub-pipeline level flow control is achieved by simply changing where a JCas is registered in the metadata store. Since CRS-aware CRs are also responsible for removing a JCas from the CRS once it has finished processing, this means that the CRS should only have JCas in its repository and metadata store that need to be processed. So JCases that failed to process should remain in the CRS and be processed during the next processing cycle.

## Usage

To see an example usage of the CRS, look at the [integration test](). Overall, the process is very simple:

The CRS-aware AEs need to create a proper protobuf [Message](https://github.com/revistek/cas-repository-service-common/blob/main/src/main/proto/Message.proto) object with the appropriately encased protobuf [Cas](https://github.com/revistek/cas-repository-service-common/blob/main/src/main/proto/Cas.proto) object and call the `/rest/store` endpoint to store the JCas. The AEs should also have logic to decide where in the metadata store to register the JCas. The CRS stores the JCas in its repository in binary form, generates a Cas ID for that JCas, and registers that ID in its metadata store.

The CRS-aware CRs retrieve the Cas IDs that they need to process directly from the CRS's metadata store. With the Cas IDs in hand, they construct proper protobuf [Message](https://github.com/revistek/cas-repository-service-common/blob/main/src/main/proto/Message.proto) objects and passes them to the `/rest/get` endpoint to retrieve the JCases. Then, they send the reconstituted JCasas to their respective pipelines for processing. For each JCas, when the CRs are notified that the JCas has finished processing, the CR constructs the proper protobuf [Message](https://github.com/revistek/cas-repository-service-common/blob/main/src/main/proto/Message.proto) object and calls the `/rest/delete/` endpoint to delete the JCas from the CRS.

## Scaling

Scaling the CRS is very straight-forward. The CRS has four components: the REST endpoints, the repository, the metadata store, and the cache. In this implementation, MongoDB is used for the repository and metadata store. REDIS is used for the cache. Scaling MongoDB and REDIS is beyond the scope of this write up, but there are plenty of resources readily available. The REST endpoints are just web services and can be scaled by standing up multiple instances and using a load balancer.
