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

    # LDAP group-based role mapping (optional)
    config["role_mapping_mode"] = os.environ.get("MEMGRAPH_SSO_KERBEROS_ROLE_MAPPING_MODE", "ldap")
    config["ldap_uri"] = os.environ.get("MEMGRAPH_SSO_KERBEROS_LDAP_URI", "")
    config["ldap_base_dn"] = os.environ.get("MEMGRAPH_SSO_KERBEROS_LDAP_BASE_DN", "")
    config["ldap_search_base"] = os.environ.get("MEMGRAPH_SSO_KERBEROS_LDAP_SEARCH_BASE", "")
    return config


def _resolve_roles_principal(client_principal: str, role_mapping: dict) -> dict:
    """Map roles by matching the Kerberos principal name against static mappings."""
    roles = []
    principal_name = client_principal.split("@")[0] if "@" in client_principal else client_principal
    for idp_role, mg_roles in role_mapping.items():
        if idp_role == client_principal or idp_role == principal_name or idp_role == "*":
            roles.extend(mg_roles)
    return {"roles": roles}


def _resolve_roles_ldap(client_principal: str, role_mapping: dict, config: dict) -> dict:
    """Query AD/LDAP for the user's group membership, then map groups to roles."""
    try:
        import ldap3
    except ImportError:
        return {"error": "ldap3 package not installed. Run: pip install ldap3"}

    ldap_uri = config["ldap_uri"]
    if not ldap_uri:
        return {"error": "MEMGRAPH_SSO_KERBEROS_LDAP_URI is required when role_mapping_mode is 'ldap'"}

    base_dn = config["ldap_base_dn"]
    if not base_dn:
        return {"error": "MEMGRAPH_SSO_KERBEROS_LDAP_BASE_DN is required when role_mapping_mode is 'ldap'"}

    principal_name = client_principal.split("@")[0] if "@" in client_principal else client_principal
    search_base = config["ldap_search_base"] or base_dn

    try:
        server = ldap3.Server(ldap_uri, get_info=ldap3.ALL)
        conn = ldap3.Connection(server, authentication=ldap3.SASL, sasl_mechanism=ldap3.KERBEROS)
        if not conn.bind():
            return {"error": f"LDAP bind failed: {conn.result}"}

        # Search for the user by sAMAccountName and read their memberOf attribute
        search_filter = f"(&(objectClass=user)(sAMAccountName={ldap3.utils.conv.escape_filter_chars(principal_name)}))"
        conn.search(search_base, search_filter, attributes=["memberOf"])

        if not conn.entries:
            return {"error": f"User {principal_name} not found in LDAP under {search_base}"}

        member_of = conn.entries[0].memberOf.values if conn.entries[0].memberOf else []
        conn.unbind()
    except Exception as e:
        return {"error": f"LDAP query failed: {str(e)}"}

    # Extract CN from group DNs: "CN=mg-admins,OU=Groups,DC=corp,DC=com" → "mg-admins"
    user_groups = set()
    for group_dn in member_of:
        for rdn in group_dn.split(","):
            if rdn.strip().upper().startswith("CN="):
                user_groups.add(rdn.strip()[3:])
                break

    # Match groups against role mapping
    roles = []
    for mapping_key, mg_roles in role_mapping.items():
        if mapping_key in user_groups or mapping_key == "*":
            roles.extend(mg_roles)

    return {"roles": roles}


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

    role_mapping = config["role_mapping"]
    role_mapping_mode = config["role_mapping_mode"]

    if role_mapping_mode == "ldap":
        resolved = _resolve_roles_ldap(client_principal, role_mapping, config)
    else:
        resolved = _resolve_roles_principal(client_principal, role_mapping)

    if "error" in resolved:
        return {"authenticated": False, "errors": resolved["error"]}
    roles = resolved["roles"]

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
