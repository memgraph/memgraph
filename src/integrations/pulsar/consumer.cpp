// Copyright 2021 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "integrations/pulsar/consumer.hpp"

#include <pulsar/Client.h>

#include <chrono>
#include <thread>

namespace integrations::pulsar {
void test() {
  ::pulsar::Client client("pulsar://localhost:6650");

  {
    ::pulsar::Producer producer;
    ::pulsar::Result result = client.createProducer("persistent://public/default/my-topic", producer);
    if (result != ::pulsar::ResultOk) {
      std::cout << "Error creating producer: " << result << std::endl;
      return;
    }
    int ctr = 0;
    while (ctr < 100) {
      std::string content = "msg" + std::to_string(ctr);
      ::pulsar::Message msg = ::pulsar::MessageBuilder().setContent(content).setProperty("x", "1").build();
      ::pulsar::Result result = producer.send(msg);
      if (result != ::pulsar::ResultOk) {
        std::cout << "The message " << content << " could not be sent, received code: " << result << std::endl;
      } else {
        std::cout << "The message " << content << " sent successfully" << std::endl;
      }

      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      ctr++;
    }

    std::cout << "Finished producing synchronously!" << std::endl;

    client.close();
  }
}
}  // namespace integrations::pulsar
