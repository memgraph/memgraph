// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <cstring>  //memset

#include <iostream>
#include <string>
#include <vector>

using std::string;
using std::vector;

vector<string> dns_lookup(const string &host_name, int ipv = 4);
vector<string> dns_lookup_test(const string &host_name, int ipv = 4);

int main(int argc, char *argv[]) {
  std::cout << "!!!! dns lookup" << std::endl;
  vector<string> domains;

  std::cout << "dns ipv4: www.google.com" << std::endl;
  domains = dns_lookup("www.google.com", 4);
  for (int i = 0, ix = domains.size(); i < ix; i++) {
    std::cout << " " << domains[i] << std::endl;
  }

  std::cout << "dns ipv6: www.google.com" << std::endl;
  domains = dns_lookup("www.google.com", 6);
  for (int i = 0, ix = domains.size(); i < ix; i++) {
    std::cout << " " << domains[i] << std::endl;
  }

  std::cout << "dns both: www.google.com" << std::endl;
  domains = dns_lookup("www.google.com", 0);
  for (int i = 0, ix = domains.size(); i < ix; i++) {
    std::cout << " " << domains[i] << std::endl;
  }

  domains = dns_lookup("memgraph-replica-0.memgraph-replica.my-namespace.my-cluster.my-dc.proxy.net", 0);
  std::cout << "!!!!!!!!!" << std::endl;
  for (int i = 0, ix = domains.size(); i < ix; i++) {
    std::cout << " " << domains[i] << std::endl;
  }

  domains = dns_lookup("127.0.0.1", 0);
  std::cout << "!!!!!!!!!" << std::endl;
  for (int i = 0, ix = domains.size(); i < ix; i++) {
    std::cout << " " << domains[i] << std::endl;
  }

  std::cout << "!!!! dns lookup test" << std::endl;

  std::cout << "dns ipv4: www.google.com" << std::endl;
  domains = dns_lookup_test("www.google.com", 0);
  for (int i = 0, ix = domains.size(); i < ix; i++) {
    std::cout << " " << domains[i] << std::endl;
  }

  std::cout << "dns ipv6: www.google.com" << std::endl;
  domains = dns_lookup_test("www.google.com", 0);
  for (int i = 0, ix = domains.size(); i < ix; i++) {
    std::cout << " " << domains[i] << std::endl;
  }

  std::cout << "dns both: www.google.com" << std::endl;
  domains = dns_lookup_test("www.google.com", 0);
  for (int i = 0, ix = domains.size(); i < ix; i++) {
    std::cout << " " << domains[i] << std::endl;
  }

  domains = dns_lookup_test("memgraph-replica-0.memgraph-replica.my-namespace.my-cluster.my-dc.proxy.net", 0);
  std::cout << "!!!!!!!!!" << std::endl;
  for (int i = 0, ix = domains.size(); i < ix; i++) {
    std::cout << " " << domains[i] << std::endl;
  }

  domains = dns_lookup_test("127.0.0.1", 0);
  std::cout << "!!!!!!!!!" << std::endl;
  for (int i = 0, ix = domains.size(); i < ix; i++) {
    std::cout << " " << domains[i] << std::endl;
  }

  return 0;
}

vector<string> dns_lookup(const string &host_name, int ipv) {
  vector<string> output{};

  addrinfo hints{};
  addrinfo *res, *p;
  int ai_family;
  char ip_address[INET6_ADDRSTRLEN];

  ai_family = ipv == 6 ? AF_INET6 : AF_INET;     // v4 vs v6?
  ai_family = ipv == 0 ? AF_UNSPEC : ai_family;  // AF_UNSPEC (any), or chosen
  hints.ai_family = ai_family;
  hints.ai_socktype = SOCK_STREAM;

  if (int status = getaddrinfo(host_name.c_str(), nullptr, &hints, &res); status != 0) {
    // cerr << "getaddrinfo: "<< gai_strerror(status) << endl;
    return output;
  }

  std::cout << "DNS Lookup: " << host_name << " ipv:" << ipv << std::endl;

  for (p = res; p != nullptr; p = p->ai_next) {
    void *addr;
    if (p->ai_family == AF_INET) {  // IPv4
      struct sockaddr_in *ipv4 = (struct sockaddr_in *)p->ai_addr;
      addr = &(ipv4->sin_addr);
    } else {  // IPv6
      struct sockaddr_in6 *ipv6 = (struct sockaddr_in6 *)p->ai_addr;
      addr = &(ipv6->sin6_addr);
    }

    // convert the IP to a string
    inet_ntop(p->ai_family, addr, ip_address, sizeof ip_address);
    output.emplace_back(ip_address);
  }

  freeaddrinfo(res);  // free the linked list

  return output;
}

vector<string> dns_lookup_test(const string &host_name, int ipv) {
  vector<string> output{};

  addrinfo hints{};
  addrinfo *res, *p;
  char ip_address[INET6_ADDRSTRLEN];

  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;

  if (int status = getaddrinfo(host_name.c_str(), nullptr, &hints, &res); status != 0) {
    return output;
  }

  std::cout << "DNS Lookup: " << host_name << " ipv:" << ipv << std::endl;

  for (p = res; p != nullptr; p = p->ai_next) {
    void *addr{nullptr};
    switch (p->ai_family) {
      case AF_UNSPEC:
        break;
      case AF_INET: {
        struct sockaddr_in *socket_address_ipv4 = (struct sockaddr_in *)p->ai_addr;
        addr = &(socket_address_ipv4->sin_addr);
        break;
      }
      case AF_INET6: {
        struct sockaddr_in6 *socket_address_ipv6 = (struct sockaddr_in6 *)p->ai_addr;
        addr = &(socket_address_ipv6->sin6_addr);
        break;
      }
      default:
        throw std::runtime_error("Can't parse");
    }

    if (nullptr == addr) {
      continue;
    }

    inet_ntop(p->ai_family, addr, ip_address, sizeof(ip_address));
    output.emplace_back(ip_address);
  }

  freeaddrinfo(res);  // free the linked list

  return output;
}
