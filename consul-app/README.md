# The Consul App

The Kafka Snow White consul app lets you configure the Kafka mirrors via keys in the [Consul](https://www.consul.io/) KV-store. 

The app is provided as an executable JAR (see how to obtain it in the [main README](../README.md)) and can be started with the following command:
```
export KAFKA_SNOW_WHITE_CONSUL_URL=http://localhost:8500
export KAFKA_SNOW_WHITE_CONSUL_ROOT_KEY=some/consul/key

java -jar consul-app.jar
```

(where we assume that the app JAR was renamed to `consul-app.jar`)

This will start the service and will look for mirror definitions in the `some/consul/key` key in the Consul instance that resides in `http://localhost:8500`.

Alternatively, we can start the application with a configuration file instead of using environment variables, like so:
```
java -Dconfig.file=path/to/some-conf-file.conf -jar consul-app.jar 
```

Where `some-conf-file.conf` has the following content (in [HOCON format](https://github.com/lightbend/config/blob/master/HOCON.md)):
```
include classpath("application")

kafka-mirror-settings = {
  consul-settings = {
    url = "http://localhost:8500"

    root-key = some/consul/key
  }
}
```

Each mirror definition needs to reside in a single key under the root key specifid above (see for the configuration format [main README](../README.md)). So if we have key structure:
```
some/consul/key
  |- mirror1
  |- mirror2
```

Once the service is started we will have two mirrors named `mirror1` and `mirror2` running.

The service will listen to changes in the root key and if any of the mirror definitions are updated/removed/added the service will dynamically update its configuration accordingly.
