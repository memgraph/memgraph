#!/usr/bin/python3

import base64
import io
import json
import os


def _load_role_mappings(raw_role_mappings: str) -> dict:
    if raw_role_mappings:
        role_mapping = {}
        for mapping in raw_role_mappings.strip().split(";"):
            if not mapping.strip():
                continue
            mapping_list = mapping.split(":")
            if len(mapping_list) != 2:
                raise ValueError(f"Invalid role mapping: {mapping}")
            idp_role, mg_roles = mapping_list
            roles = [role.strip() for role in mg_roles.split(",") if role.strip()]
            if not roles:
                raise ValueError(f"No valid roles specified for: {idp_role}")
            role_mapping[idp_role.strip()] = roles
        return role_mapping
    raise ValueError("Missing role mappings")


def _load_config_from_env():
    config = {}
    config["keytab"] = os.environ.get("MEMGRAPH_SSO_KERBEROS_KEYTAB", "")
    config["service_principal"] = os.environ.get("MEMGRAPH_SSO_KERBEROS_SERVICE_PRINCIPAL", "")
    config["realm"] = os.environ.get("MEMGRAPH_SSO_KERBEROS_REALM", "")
    config["username_field"] = os.environ.get("MEMGRAPH_SSO_KERBEROS_USERNAME_FIELD", "name")
    config["role_mapping"] = _load_role_mappings(os.environ.get("MEMGRAPH_SSO_KERBEROS_ROLE_MAPPING", ""))
    return config


def authenticate(response: str, scheme: str):
    if scheme != "kerberos":
        return {"authenticated": False, "errors": "Invalid SSO scheme"}

    try:
        config = _load_config_from_env()
    except Exception as e:
        return {"authenticated": False, "errors": f"Configuration error: {' '.join(e.args)}"}

    keytab_path = config["keytab"]
    if keytab_path:
        os.environ["KRB5_KTNAME"] = keytab_path
    else:
        os.environ.pop("KRB5_KTNAME", None)

    service_principal = config["service_principal"]
    if not service_principal:
        return {"authenticated": False, "errors": "Missing service principal configuration"}

    try:
        token_bytes = base64.b64decode(response)
    except Exception as e:
        return {"authenticated": False, "errors": f"Failed to decode base64 token: {str(e)}"}

    try:
        import gssapi
    except ImportError:
        return {"authenticated": False, "errors": "gssapi package not installed. Run: pip install gssapi"}

    try:
        server_name = gssapi.Name(service_principal, gssapi.NameType.kerberos_principal)
        server_creds = gssapi.Credentials(name=server_name, usage="accept")
        ctx = gssapi.SecurityContext(creds=server_creds, usage="accept")
        ctx.step(token_bytes)

        if not ctx.complete:
            return {"authenticated": False, "errors": "GSSAPI security context negotiation incomplete"}

        client_principal = str(ctx.initiator_name)
    except gssapi.exceptions.GSSError as e:
        return {"authenticated": False, "errors": f"Kerberos authentication failed: {str(e)}"}

    username_field = config["username_field"]
    if username_field == "principal":
        username = client_principal
    elif username_field == "name":
        username = client_principal.split("@")[0] if "@" in client_principal else client_principal
    else:
        return {"authenticated": False, "errors": f"Invalid username_field: {username_field}"}

    realm = config["realm"]
    if realm and "@" in client_principal:
        client_realm = client_principal.split("@")[1]
        if client_realm != realm:
            return {
                "authenticated": False,
                "errors": f"Client realm {client_realm} does not match expected realm {realm}",
            }

    roles = []
    role_mapping = config["role_mapping"]
    principal_name = client_principal.split("@")[0] if "@" in client_principal else client_principal
    for idp_role, mg_roles in role_mapping.items():
        if idp_role == client_principal or idp_role == principal_name or idp_role == "*":
            roles.extend(mg_roles)

    if not roles:
        return {
            "authenticated": False,
            "errors": f"Cannot map principal {client_principal} to any Memgraph role",
        }

    return {
        "authenticated": True,
        "username": username,
        "roles": list(set(roles)),
    }


if __name__ == "__main__":
    from common import add_call_id_to_response, pop_call_id

    # Stateless: each request is independent. Do not cache tokens or user data,
    # or the next connection may get the previous user (wrong-user bug).
    input_stream = io.FileIO(1000, mode="r")
    output_stream = io.FileIO(1001, mode="w")
    while True:
        params = json.loads(input_stream.readline().decode("ascii"))
        call_id = pop_call_id(params)
        ret = authenticate(**params)
        add_call_id_to_response(ret, call_id)
        output_stream.write((json.dumps(ret) + "\n").encode("ascii"))
