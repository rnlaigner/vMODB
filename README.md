# vMODB — Virtual Microservice‑Oriented Database System

![Java](https://img.shields.io/badge/Java-21-blue.svg) ![Maven](https://img.shields.io/badge/Maven-3.9%2B-blue.svg) ![Branch](https://img.shields.io/badge/branch-multi__vms-purple.svg) [![arXiv](https://img.shields.io/badge/arXiv-2504.19757-b31b1b.svg)](https://arxiv.org/abs/2504.19757)

vMODB is a distributed, event-driven, microservice-oriented database management system. The first principled approach for designing and implementing microservices that require advanced data management requirements. vMODB unifies event and data management, offering event-driven functionalities and ACID guarantees by design, making it easier for developers to build scalable microservices to run in the cloud.

Differently from traditional server-based database and message systems, where users interact via a well-defined network protocol, in vMODB, users solely write application code and all the data and event management complexity is abstracted away. For that, vMODB offers familiar programming abstractions to developers, including object-relational mapping and metaprogramming (i.e., annotations). 

In the end, developers experience the same flexibility and dynamicity offered by microservice architectures, while enjoying native system-level data management support that effectively prevents several challenges usually found in the practice.

## Table of Contents
- [Why vMODB](#why-vmodb)
- [Quickstart](#quickstart)
    * [Prerequisites](#prerequisites)
    * [Build](#build)
    * [Configuration](#config)
- [System](#system)
    * [Abstractions](#abstractions)
    * [Architecture](#architecture)
    * [APIs](#apis)
    * [Play Around](#play)
    * [Testing](#test)
- [Troubleshooting](#troubleshooting)

### <a name="why-vmodb"></a>Why vMODB?

Event‑driven microservice architectures (EDMAs) allow teams to build systems formed by self-contained components that can be deployed, scaled, and upgraded independently. To achieve such non-functional requirements, EDMAs typically rely on asynchronous messages to enable interaction across components. While decoupling components in time facilitate software teams to move fast and adapt the system to varied workloads, ensuring transactional guarantees across components (e.g., workflow atomicity) is often perceived as a major challenge. In most cases, EDMAs end up relying on weaker guarantees, such as eventual consistency, in order to achieve performance requirements like scalability.

vMODB departs from traditional EDMAs by providing a programming model (VMS) and a runtime that unifies event logs and state to deliver ACID across microservices. In evaluations, vMODB outperforms widely adopted eventual‑consistency frameworks by up to 3x.

### <a name="quickstart"></a>Quickstart

#### <a name="prerequisites"></a>Prerequisites

- Java Development Kit (JDK) 21 by any distributor, such as [OpenJDK](https://jdk.java.net/archive/)
- Maven to assemble the dependencies and compile the project: [Maven](https://www.hostinger.com/tutorials/how-to-install-maven-on-ubuntu)
- Curl to play with the HTTP APIs (Optional)

#### <a name="build"></a>Build

Before building the project, clone the source code repository:

```
git clone --depth 1 https://github.com/rnlaigner/vMODB
cd vMODB
```

It is necessary to generate the dependencies required to compile the microservice.
This can be accomplished via running the following command in the root folder:

```
mvn clean install -DskipTests=true
```

Then you can just run the following command:
```
mvn clean package -DskipTests=true
```

To run, test, and debug applications in an IDE like IntelliJ:

To run the Online Marketplace benchmark, for each submodule under `marketplace` , use the following VM parameters:
```
--enable-preview
--add-exports
java.base/jdk.internal.misc=ALL-UNNAMED
--add-opens
java.base/jdk.internal.misc=ALL-UNNAMED
```

To run TPC-C, for each submodule under `tpcc` , use the following VM parameters:
```
-XX:+UseParallelGC
--enable-preview
--add-exports
java.base/jdk.internal.misc=ALL-UNNAMED
--add-opens
java.base/jdk.internal.util=ALL-UNNAMED
--add-opens
java.base/java.nio=ALL-UNNAMED
--add-opens
java.base/sun.nio.ch=ALL-UNNAMED
```

To profile the system, use the following VM parameters:
```
-XX:StartFlightRecording=filename=app.jfr,settings=profile,dumponexit=true
-XX:+HeapDumpOnOutOfMemoryError
-XX:HeapDumpPath=/tmp/heapdump.hprof
-XX:+UseParallelGC
--enable-preview
--add-exports
java.base/jdk.internal.misc=ALL-UNNAMED
--add-opens
java.base/jdk.internal.util=ALL-UNNAMED
--add-opens
java.base/java.nio=ALL-UNNAMED
--add-opens
java.base/sun.nio.ch=ALL-UNNAMED
```

### <a name="system"></a>System

#### <a name="architecture"></a>Architecture

At the core of vMODB lies the virtual micro service (VMS) programming model. Through a VMS, users define a component’s data model, constraints, and concurrency semantics. vMODB coordinates the execution of a collection of VMS instances. Developers specify transactions that may traverse multiple VMSes; the coordinator orders, validates, and commits them, preserving ACID across components while retaining the decoupling benefits of EDA.

## <a name="troubleshooting"></a>Troubleshooting

- [Packet Size ,Window Size and Socket Buffer In TCP](https://stackoverflow.com/a/37267929/7735153)
- [Throughput and TCP windows](http://packetbomb.com/understanding-throughput-and-tcp-windows/)
- [Tuning the window size](https://docs.oracle.com/cd/E23507_01/Platform.20073/ATGInstallGuide/html/s0507tuningthetcpwindowsize01.html)