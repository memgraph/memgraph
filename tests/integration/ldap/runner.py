#!/usr/bin/python3 -u

# Copyright 2021 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

import argparse
import atexit
import os
import stat
import subprocess
import sys
import tempfile
import time

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
PROJECT_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", "..", ".."))

CONFIG_TEMPLATE = """
server:
  host: "127.0.0.1"
  port: 1389
  encryption: "{encryption}"
  cert_file: ""
  key_file: ""
  ca_file: ""
  validate_cert: false

users:
  prefix: "{prefix}"
  suffix: "{suffix}"

roles:
  root_dn: "{root_dn}"
  root_objectclass: "{root_objectclass}"
  user_attribute: "{user_attribute}"
  role_attribute: "{role_attribute}"
"""


def wait_for_server(port, delay=0.1):
    cmd = ["nc", "-z", "-w", "1", "127.0.0.1", str(port)]
    while subprocess.call(cmd) != 0:
        time.sleep(0.01)
    time.sleep(delay)


def execute_tester(binary, queries, username="", password="",
                   auth_should_fail=False, query_should_fail=False):
    if password == "":
        password = username
    args = [binary, "--username", username, "--password", password]
    if auth_should_fail:
        args.append("--auth-should-fail")
    if query_should_fail:
        args.append("--query-should-fail")
    args.extend(queries)
    subprocess.run(args).check_returncode()


class Memgraph:
    def __init__(self, binary):
        self._binary = binary
        self._storage_directory = None
        self._auth_module = None
        self._auth_config = None
        self._process = None

    def start(self, **kwargs):
        self.stop()
        self._storage_directory = tempfile.TemporaryDirectory()
        self._auth_module = os.path.join(self._storage_directory.name,
                                         "ldap.py")
        self._auth_config = os.path.join(self._storage_directory.name,
                                         "ldap.yaml")
        script_file = os.path.join(PROJECT_DIR, "src", "auth",
                                   "reference_modules", "ldap.py")
        virtualenv_bin = os.path.join(SCRIPT_DIR, "ve3", "bin", "python3")
        with open(script_file) as fin:
            data = fin.read()
            data = data.replace("/usr/bin/python3", virtualenv_bin)
            data = data.replace("/etc/memgraph/auth/ldap.yaml",
                                self._auth_config)
            with open(self._auth_module, "w") as fout:
                fout.write(data)
        os.chmod(self._auth_module, stat.S_IRWXU | stat.S_IRWXG)
        self.restart(**kwargs)

    def restart(self, **kwargs):
        self.stop()
        config = {
            "encryption": kwargs.pop("encryption", "disabled"),
            "prefix": kwargs.pop("prefix", "cn="),
            "suffix": kwargs.pop("suffix", ",ou=people,dc=memgraph,dc=com"),
            "root_dn": kwargs.pop("root_dn", "ou=roles,dc=memgraph,dc=com"),
            "root_objectclass": kwargs.pop("root_objectclass", "groupOfNames"),
            "user_attribute": kwargs.pop("user_attribute", "member"),
            "role_attribute": kwargs.pop("role_attribute", "cn"),
        }
        with open(self._auth_config, "w") as f:
            f.write(CONFIG_TEMPLATE.format(**config))
        args = [self._binary,
                "--data-directory", self._storage_directory.name,
                "--auth-module-executable",
                kwargs.pop("module_executable", self._auth_module)]
        for key, value in kwargs.items():
            ldap_key = "--auth-module-" + key.replace("_", "-")
            if isinstance(value, bool):
                args.append(ldap_key + "=" + str(value).lower())
            else:
                args.append(ldap_key)
                args.append(value)
        self._process = subprocess.Popen(args)
        time.sleep(0.1)
        assert self._process.poll() is None, "Memgraph process died " \
            "prematurely!"
        wait_for_server(7687)

    def stop(self, check=True):
        if self._process is None:
            return 0
        self._process.terminate()
        exitcode = self._process.wait()
        self._process = None
        if check:
            assert exitcode == 0, "Memgraph process didn't exit cleanly!"
        return exitcode


def initialize_test(memgraph, tester_binary, **kwargs):
    memgraph.start(module_executable="")

    execute_tester(tester_binary,
                   ["CREATE USER root", "GRANT ALL PRIVILEGES TO root"])
    check_login = kwargs.pop("check_login", True)
    memgraph.restart(**kwargs)
    if check_login:
        execute_tester(tester_binary, [], "root")


# Tests


def test_basic(memgraph, tester_binary):
    initialize_test(memgraph, tester_binary)
    execute_tester(tester_binary, [], "alice")
    execute_tester(tester_binary, ["GRANT MATCH TO alice"], "root")
    execute_tester(tester_binary, ["MATCH (n) RETURN n"], "alice")
    memgraph.stop()


def test_only_existing_users(memgraph, tester_binary):
    initialize_test(memgraph, tester_binary, create_missing_user=False)
    execute_tester(tester_binary, [], "alice", auth_should_fail=True)
    execute_tester(tester_binary, ["CREATE USER alice"], "root")
    execute_tester(tester_binary, [], "alice")
    execute_tester(tester_binary, ["GRANT MATCH TO alice"], "root")
    execute_tester(tester_binary, ["MATCH (n) RETURN n"], "alice")
    memgraph.stop()


def test_role_mapping(memgraph, tester_binary):
    initialize_test(memgraph, tester_binary)

    execute_tester(tester_binary, [], "alice")
    execute_tester(tester_binary, ["MATCH (n) RETURN n"], "alice",
                   query_should_fail=True)
    execute_tester(tester_binary, ["GRANT MATCH TO moderator"], "root")
    execute_tester(tester_binary, ["MATCH (n) RETURN n"], "alice")

    execute_tester(tester_binary, [], "bob")
    execute_tester(tester_binary, ["MATCH (n) RETURN n"], "bob",
                   query_should_fail=True)

    execute_tester(tester_binary, [], "carol")
    execute_tester(tester_binary, ["CREATE (n) RETURN n"], "carol",
                   query_should_fail=True)
    execute_tester(tester_binary, ["GRANT CREATE TO admin"], "root")
    execute_tester(tester_binary, ["CREATE (n) RETURN n"], "carol")
    execute_tester(tester_binary, ["CREATE (n) RETURN n"], "dave")

    memgraph.stop()


def test_role_removal(memgraph, tester_binary):
    initialize_test(memgraph, tester_binary)
    execute_tester(tester_binary, [], "alice")
    execute_tester(tester_binary, ["MATCH (n) RETURN n"], "alice",
                   query_should_fail=True)
    execute_tester(tester_binary, ["GRANT MATCH TO moderator"], "root")
    execute_tester(tester_binary, ["MATCH (n) RETURN n"], "alice")
    memgraph.restart(manage_roles=False)
    execute_tester(tester_binary, ["MATCH (n) RETURN n"], "alice")
    execute_tester(tester_binary, ["CLEAR ROLE FOR alice"], "root")
    execute_tester(tester_binary, ["MATCH (n) RETURN n"], "alice",
                   query_should_fail=True)
    memgraph.stop()


def test_only_existing_roles(memgraph, tester_binary):
    initialize_test(memgraph, tester_binary, create_missing_role=False)
    execute_tester(tester_binary, [], "bob")
    execute_tester(tester_binary, [], "alice", auth_should_fail=True)
    execute_tester(tester_binary, ["CREATE ROLE moderator"], "root")
    execute_tester(tester_binary, [], "alice")
    memgraph.stop()


def test_role_is_user(memgraph, tester_binary):
    initialize_test(memgraph, tester_binary)
    execute_tester(tester_binary, [], "admin")
    execute_tester(tester_binary, [], "carol", auth_should_fail=True)
    memgraph.stop()


def test_user_is_role(memgraph, tester_binary):
    initialize_test(memgraph, tester_binary)
    execute_tester(tester_binary, [], "carol")
    execute_tester(tester_binary, [], "admin", auth_should_fail=True)
    memgraph.stop()


def test_user_permissions_persistence(memgraph, tester_binary):
    initialize_test(memgraph, tester_binary)
    execute_tester(tester_binary,
                   ["CREATE USER alice", "GRANT MATCH TO alice"], "root")
    execute_tester(tester_binary, ["MATCH (n) RETURN n"], "alice")
    memgraph.stop()


def test_role_permissions_persistence(memgraph, tester_binary):
    initialize_test(memgraph, tester_binary)
    execute_tester(tester_binary,
                   ["CREATE ROLE moderator", "GRANT MATCH TO moderator"],
                   "root")
    execute_tester(tester_binary, ["MATCH (n) RETURN n"], "alice")
    memgraph.stop()


def test_only_authentication(memgraph, tester_binary):
    initialize_test(memgraph, tester_binary, manage_roles=False)
    execute_tester(tester_binary,
                   ["CREATE ROLE moderator", "GRANT MATCH TO moderator"],
                   "root")
    execute_tester(tester_binary, ["MATCH (n) RETURN n"], "alice",
                   query_should_fail=True)
    memgraph.stop()


def test_wrong_prefix(memgraph, tester_binary):
    initialize_test(memgraph, tester_binary, prefix="eve", check_login=False)
    execute_tester(tester_binary, [], "root", auth_should_fail=True)
    memgraph.stop()


def test_wrong_suffix(memgraph, tester_binary):
    initialize_test(memgraph, tester_binary, suffix="", check_login=False)
    execute_tester(tester_binary, [], "root", auth_should_fail=True)
    memgraph.stop()


def test_suffix_with_spaces(memgraph, tester_binary):
    initialize_test(memgraph, tester_binary,
                    suffix=",    ou= people,  dc = memgraph, dc =   com")
    execute_tester(tester_binary,
                   ["CREATE USER alice", "GRANT MATCH TO alice"], "root")
    execute_tester(tester_binary, ["MATCH (n) RETURN n"], "alice")
    memgraph.stop()


def test_role_mapping_wrong_root_dn(memgraph, tester_binary):
    initialize_test(memgraph, tester_binary,
                    root_dn="ou=invalid,dc=memgraph,dc=com")
    execute_tester(tester_binary,
                   ["CREATE ROLE moderator", "GRANT MATCH TO moderator"],
                   "root")
    execute_tester(tester_binary, ["MATCH (n) RETURN n"], "alice",
                   query_should_fail=True)
    memgraph.restart()
    execute_tester(tester_binary, ["MATCH (n) RETURN n"], "alice")
    memgraph.stop()


def test_role_mapping_wrong_root_objectclass(memgraph, tester_binary):
    initialize_test(memgraph, tester_binary, root_objectclass="person")
    execute_tester(tester_binary,
                   ["CREATE ROLE moderator", "GRANT MATCH TO moderator"],
                   "root")
    execute_tester(tester_binary, ["MATCH (n) RETURN n"], "alice",
                   query_should_fail=True)
    memgraph.restart()
    execute_tester(tester_binary, ["MATCH (n) RETURN n"], "alice")
    memgraph.stop()


def test_role_mapping_wrong_user_attribute(memgraph, tester_binary):
    initialize_test(memgraph, tester_binary, user_attribute="cn")
    execute_tester(tester_binary,
                   ["CREATE ROLE moderator", "GRANT MATCH TO moderator"],
                   "root")
    execute_tester(tester_binary, ["MATCH (n) RETURN n"], "alice",
                   query_should_fail=True)
    memgraph.restart()
    execute_tester(tester_binary, ["MATCH (n) RETURN n"], "alice")
    memgraph.stop()


def test_wrong_password(memgraph, tester_binary):
    initialize_test(memgraph, tester_binary)
    execute_tester(tester_binary, [], "root", password="sudo",
                   auth_should_fail=True)
    execute_tester(tester_binary, ["SHOW USERS"], "root", password="root")
    memgraph.stop()


def test_password_persistence(memgraph, tester_binary):
    initialize_test(memgraph, tester_binary, check_login=False)
    memgraph.restart(module_executable="")
    execute_tester(tester_binary, ["SHOW USERS"], "root", password="sudo")
    execute_tester(tester_binary, ["SHOW USERS"], "root", password="root")
    memgraph.restart()
    execute_tester(tester_binary, [], "root", password="sudo",
                   auth_should_fail=True)
    execute_tester(tester_binary, ["SHOW USERS"], "root", password="root")
    memgraph.restart(module_executable="")
    execute_tester(tester_binary, [], "root", password="sudo",
                   auth_should_fail=True)
    execute_tester(tester_binary, ["SHOW USERS"], "root", password="root")
    memgraph.stop()


def test_user_multiple_roles(memgraph, tester_binary):
    initialize_test(memgraph, tester_binary, check_login=False)
    memgraph.restart()
    execute_tester(tester_binary, ["MATCH (n) RETURN n"], "eve",
                   query_should_fail=True)
    execute_tester(tester_binary, ["GRANT MATCH TO moderator"], "root",
                   query_should_fail=True)
    memgraph.restart(manage_roles=False)
    execute_tester(tester_binary, ["MATCH (n) RETURN n"], "eve",
                   query_should_fail=True)
    execute_tester(tester_binary, ["GRANT MATCH TO moderator"], "root",
                   query_should_fail=True)
    memgraph.restart(manage_roles=False, root_dn="")
    execute_tester(tester_binary, ["MATCH (n) RETURN n"], "eve",
                   query_should_fail=True)
    execute_tester(tester_binary, ["GRANT MATCH TO moderator"], "root",
                   query_should_fail=True)
    memgraph.stop()


def test_starttls_failure(memgraph, tester_binary):
    initialize_test(memgraph, tester_binary, encryption="starttls",
                    check_login=False)
    execute_tester(tester_binary, [], "root", auth_should_fail=True)
    memgraph.stop()


def test_ssl_failure(memgraph, tester_binary):
    initialize_test(memgraph, tester_binary, encryption="ssl",
                    check_login=False)
    execute_tester(tester_binary, [], "root", auth_should_fail=True)
    memgraph.stop()


# Startup logic


if __name__ == "__main__":
    memgraph_binary = os.path.join(PROJECT_DIR, "build", "memgraph")
    tester_binary = os.path.join(PROJECT_DIR, "build", "tests",
                                 "integration", "ldap", "tester")

    parser = argparse.ArgumentParser()
    parser.add_argument("--memgraph", default=memgraph_binary)
    parser.add_argument("--tester", default=tester_binary)
    parser.add_argument("--openldap-dir",
                        default=os.path.join(SCRIPT_DIR, "openldap-2.4.47"))
    args = parser.parse_args()

    # Setup Memgraph handler
    memgraph = Memgraph(args.memgraph)

    # Start the slapd binary
    slapd_args = [os.path.join(args.openldap_dir, "exe", "libexec", "slapd"),
                  "-h", "ldap://127.0.0.1:1389/", "-d", "0"]
    slapd = subprocess.Popen(slapd_args)
    time.sleep(0.1)
    assert slapd.poll() is None, "slapd process died prematurely!"
    wait_for_server(1389)

    # Register cleanup function
    @atexit.register
    def cleanup():
        mg_stat = memgraph.stop(check=False)
        if mg_stat != 0:
            print("Memgraph process didn't exit cleanly!")

        if slapd.poll() is None:
            slapd.terminate()
        slapd_stat = slapd.wait()
        if slapd_stat != 0:
            print("slapd process didn't exit cleanly!")

        assert mg_stat == 0 and slapd_stat == 0, "Some of the processes " \
            "(memgraph, slapd) crashed!"

    # Execute tests
    names = sorted(globals().keys())
    for name in names:
        if not name.startswith("test_"):
            continue
        test = " ".join(name[5:].split("_"))
        func = globals()[name]
        print("\033[1;36m~~ Running", test, "test ~~\033[0m")
        func(memgraph, args.tester)
        print("\033[1;36m~~ Finished", test, "test ~~\033[0m\n")

    # Shutdown the slapd binary
    slapd.terminate()
    assert slapd.wait() == 0, "slapd process didn't exit cleanly!"

    sys.exit(0)
