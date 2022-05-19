There are three docker-compose files in this directory:
* [kafka.yml](kafka.yml)
* [pulsar.yml](pulsar.yml)
* [redpanda.yml](redpanda.yml)

To run one of them, use the `docker-compose -f <filename> up -V` command. Optionally you can append `-d` to detach from the started containers. You can stop the detach containers by `docker-compose -f <filename> down`.

If you experience strange errors, try to clean up the previously created containers by `docker-compose -f <filename> rm -svf`.