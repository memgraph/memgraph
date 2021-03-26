#!/usr/bin/python3
import json
import io
import ssl
import sys

import ldap3
import yaml

# Load config file.
CONFIG_FILE = "/etc/memgraph/auth/ldap.yaml"
with open(CONFIG_FILE) as f:
    config = yaml.safe_load(f)
server_config = config["server"]
users_config = config["users"]
roles_config = config["roles"]

# Initialize LDAP server.
tls = None
if server_config["encryption"] != "disabled":
    cert_file = server_config["cert_file"] if server_config["cert_file"] \
        else None
    key_file = server_config["key_file"] if server_config["key_file"] else None
    ca_file = server_config["ca_file"] if server_config["ca_file"] else None
    validate = ssl.CERT_REQUIRED if server_config["validate_cert"] \
        else ssl.CERT_NONE
    tls = ldap3.Tls(local_private_key_file=key_file,
                    local_certificate_file=cert_file,
                    ca_certs_file=ca_file,
                    validate=validate)
use_ssl = server_config["encryption"] == "ssl"
server = ldap3.Server(server_config["host"], port=server_config["port"],
                      tls=tls, use_ssl=use_ssl, get_info=ldap3.ALL)


# Main authentication/authorization function.
def authenticate(username, password):
    # LDAP doesn't support empty RDNs.
    if username == "":
        return {"authenticated": False, "role": ""}

    # Create the DN of the user
    dn = users_config["prefix"] + ldap3.utils.dn.escape_rdn(username) + \
        users_config["suffix"]

    # Bind to the server
    conn = ldap3.Connection(server, dn, password)
    if server_config["encryption"] == "starttls" and not conn.start_tls():
        print("ERROR: Couldn't issue STARTTLS to the LDAP server!",
              file=sys.stderr)
        return {"authenticated": False, "role": ""}
    if not conn.bind():
        return {"authenticated": False, "role": ""}

    # Try to find out the user's role
    if roles_config["root_dn"] != "":
        # search for role
        search_filter = "(&(objectclass={objclass})({attr}={value}))".format(
                objclass=roles_config["root_objectclass"],
                attr=roles_config["user_attribute"],
                value=ldap3.utils.conv.escape_filter_chars(dn))
        succ = conn.search(roles_config["root_dn"], search_filter,
                           search_scope=ldap3.LEVEL,
                           attributes=[roles_config["role_attribute"]])
        if not succ or len(conn.entries) == 0:
            return {"authenticated": True, "role": ""}
        if len(conn.entries) > 1:
            roles = list(map(lambda x: x[roles_config["role_attribute"]].value,
                             conn.entries))
            # Because we don't know exactly which role the user should have
            # we authorize the user with an empty role.
            print("WARNING: Found more than one role for "
                  "user '" + username + "':", ", ".join(roles) + "!",
                  file=sys.stderr)
            return {"authenticated": True, "role": ""}
        return {"authenticated": True,
                "role": conn.entries[0][roles_config["role_attribute"]].value}
    else:
        return {"authenticated": True, "role": ""}


if __name__ == "__main__":
    input_stream = io.FileIO(1000, mode="r")
    output_stream = io.FileIO(1001, mode="w")
    while True:
        params = json.loads(input_stream.readline().decode("ascii"))
        ret = authenticate(**params)
        output_stream.write((json.dumps(ret) + "\n").encode("ascii"))
