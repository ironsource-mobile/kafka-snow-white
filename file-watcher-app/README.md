# The File-watcher App

The Kafka Snow White file-watcher app lets you configure the Kafka mirrors via files in the file-system. 

The app is provided as an executable JAR (see how to obtain it in the [main README](../README.md)) and can be started with the following command:
```
export KAFKA_SNOW_WHITE_MIRRORS_DIR=/some/dir/with/mirrors
java -jar file-watcher-app.jar
```

(where we assume that the app JAR was renamed to `file-watcher-app.jar`)

This will start the service and will look for mirror definitions in the `/some/dir/with/mirrors` directory.

Alternatively, we can start the application with a configuration file instead of using environment variables, like so:
```
java -Dconfig.file=path/to/some-conf-file.conf -jar file-watcher-app.jar 
```

Where `some-conf-file.conf` has the following content (in [HOCON format](https://github.com/lightbend/config/blob/master/HOCON.md)):
```
include classpath("application")

kafka-mirror-settings  {
  mirrors-directory = "/some/dir/with/mirrors"
}
```

Each mirror definition needs to reside in a single file with a `.conf` suffix (see for the configuration format [main README](../README.md)). So if we have this file structure:
```
/some/dir/with/mirrors
  |- mirror1.conf
  |- mirror2.conf
```

Once the service is started we will have two mirrors named `mirror1` and `mirror2` running.

The service will listen to directory changes and if any of the mirror definitions are updated/removed/added the service will dynamically update its configuration accordingly.
