#!/bin/bash

clang++ -g -rdynamic ../bolt/v1/states/error.cpp ../bolt/v1/states/executor.cpp ../logging/streams/stdout.cpp ../logging/levels.cpp ../logging/logs/sync_log.cpp ../logging/logs/async_log.cpp ../logging/default.cpp ../logging/log.cpp ../bolt/v1/bolt.cpp ../bolt/v1/states/init.cpp ../bolt/v1/states.cpp ../bolt/v1/states/handshake.cpp  ../bolt/v1/transport/bolt_decoder.cpp ../bolt/v1/transport/buffer.cpp ../bolt/v1/session.cpp bolt.cpp ../io/network/tls.cpp -o bolt -std=c++14 -I ../ -I ../../libs/fmt/ -pthread -lcppformat -lssl -lcrypto
