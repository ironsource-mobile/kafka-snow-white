# Kafka Snow White

[![Build Status](https://travis-ci.org/SupersonicAds/kafka-snow-white.svg?branch=master)](https://travis-ci.org/SupersonicAds/kafka-snow-white) [![Download](https://api.bintray.com/packages/ironsonic/maven/kafka-snow-white/images/download.svg) ](https://bintray.com/ironsonic/maven/kafka-snow-white/_latestVersion)

> Mirror, mirror on the wall, who is the fairest of them all?

A Kafka mirroring service based on Akka Streams Kafka. The service can be used to move messages between topics on different Kafka servers.

## Getting Kafka Snow White

Kafka Snow White is available as executable JAR files in multiple variations (see below), these can be downloaded from Bintray:
- [kafka-snow-white-file-watcher-app](https://bintray.com/ironsonic/maven/kafka-snow-white-file-watcher-app)
- [kafka-snow-white-consul-app](https://bintray.com/ironsonic/maven/kafka-snow-white-consul-app)

Pick the `xxx-assembly.jar` files where `xxx` stands for the app name and version.

The code is also available as a library and can be used via SBT with:
```scala
resolvers += Resolver.jcenterRepo

// If you want to run the service from code
libraryDependencies += "com.supersonic" %% "kafka-snow-white-consul-app" % "1.0.0"
libraryDependencies += "com.supersonic" %% "kafka-snow-white-file-watcher" % "1.0.0"

// If you want only the basic mirroring functionality with no backing mechanism
libraryDependencies += "com.supersonic" %% "kafka-snow-white-core" % "1.0.0"

// If you want to create a new backend for mirroring
libraryDependencies += "com.supersonic" %% "kafka-snow-white-app-common" % "1.0.0" 
```

## Motivation
Given two different Kafka servers (`server1` and `server2`) we want to move messages from the topic `some-topic` on `server1` to `some-topic` on `server2`.
The Kafka Snow White service lets one define a mirror, that does exactly this (see setup details below). Given the appropriate mirror, if we start the Kafka Snow White service with it, it will connect to `server1` consume all messages from `some-topic` and then produce the messages to `some-topic` on `server2`.

## Setup

The Kafka Snow White service is available in two flavors which differ in the way that they are configured.
The first takes configuration from files on the file-system and each mirror corresponds to a single file. The second, takes configuration from Consul keys in Consul's KV-store, and a mirror corresponds to a single key.
For setup instructions for each flavor, see the README files under [file-watcher-app](file-watcher-app/README.md) and [consul-app](consul-app/README.md).

### Mirror Configuration Format

The configuration schema (using the [HOCON format](https://github.com/lightbend/config/blob/master/HOCON.md)) for both flavors of the Kafka Snow White service is the same. The example mirror that was described in the motivation section above can be set up as follows:
```
consumer {
  kafka-clients {
    bootstrap.servers = "server1:9092"
    group.id = "some-group"
    auto.offset.reset = "earliest"
  }
}

producer {
  kafka-clients = {
    bootstrap.servers = "server2:9092"
  }
}

mirror {
  whitelist = ["some-topic"]
  commitBatchSize = 1000
  commitParallelism = 4
}
```

A mirror specification has three sections:
- `consumer` - setting up the consumer that will be used by the mirror (this corresponds to the source server)
- `producer` - setting up the producer that will be used by the mirror (this corresponds to the target server)
- `mirror` - settings for the mirror itself

Since the mirror is backed by the Akka Streams Kafka library, the `consumer` and `producer` settings are passed directly to it. The full specification of the available settings can be found in the Akka Streams Kafka documentation for the [consumer](https://doc.akka.io/docs/akka-stream-kafka/current/consumer.html#settings) and the [producer](https://doc.akka.io/docs/akka-stream-kafka/current/producer.html#settings).

The mirror settings are defined as follows:
```
mirror {
  # The list of topics the mirror listens to (mandatory). 
  # Unless overridden in  the `topicsToRename` field each topic in this list 
  # will be mirrored in the target server.
  whitelist = ['some-topic-1', 'some-topic-2']
  
  # How many messages to batch before committing their offsets to Kafka (optional, default 1000).
  commitBatchSize = 1000
  
  # The parallelism level used to commit the offsets (optional, default 4).
  commitParallelism = 4
  
  # Settings to enable bucketing of mirrored values (optional).
  # The (mirrorBuckets / totalBuckets) ratio is the percentage of traffic to be mirrored.
  bucketing {
    # The number of buckets that should be mirrored (mandatory, no default).
    mirrorBuckets = 4
    
    # The total number of buckets, used to calculate the percentage of traffic
    # to mirror (mandatory, no default).
    totalBuckets = 16
  }
  
  # Whether the mirror should be enabled or not (optional, default 'true').
  enabled = true
  
  # Map of source to target topics to rename when mirroring messages to the 
  # target server (optional, default empty).
  topicsToRename {
    some-topic-2 = "renamed-topic"
  }
}
```

### Topic Renaming
By default the topic `whitelist` defines topics that should be mirrored from the source server in the target server. This means that each topic in the list will be recreated with the same name in the target server. It is possible to override this behavior by providing the `topicsToRename` map. In the example above, the `whitelist` contains two topics (`some-topic-1` and `some-topic-2`) and the `topicsToRename` map contains a single entry that maps `some-topic-2` to `renamed-topic`. When the mirror will be run, the contents of `some-topic-2` in the source server will be mirrored in the target server in a new topic called `renamed-topic`. While the `some-topic-1` topic will be mirrored in the target server under the same name (since it does not have an entry in the map).

### Bucketing

It is possible to (probabilistically) mirror only some percentage of the messages in a topic, to do this we need to specify the `bucketing` settings. As in the example above, we set `mirrorBuckets` to `4` and `totalBuckets` to `16`, this means that only `4 / 16 = 25%` of the messages will be mirrored by the mirror.

The selection of which messages to mirror is based on the key of the incoming message, meaning that if a key was once mirrored it will always be mirrored when the same key is encountered again. This can be useful when mirroring traffic between production and staging/testing environments (assuming that the key is not `null` and has some logical info in it). 

Note: this implies that if the keys are all the same (e.g., `null`) then all messages will be either mirrored or not depending on the key's value.

## The Healthcheck Server

Upon startup, the Kafka Snow White service starts a healthcheck server (by default on port `8080`). The healthcheck data can be accessed via: 
```
localhost:8080/healthcheck
``` 

The route contains information about the running service including data about the currently running mirrors. This can be useful to troubleshoot the settings of a mirror when trying to set it up for the first time. Note, mirrors that failed to start or are disabled are currently not displayed.

The default port can be overridden by setting the `KAFKA_SNOW_WHITE_HEALTHCHECK_PORT` environment variable. For example:
```
export KAFKA_SNOW_WHITE_HEALTHCHECK_PORT=8081

# ... start the Kafka Snow White service
```
Or, in a configuration file:
```
kafka-mirror-settings {
  port = 8081
}
```
