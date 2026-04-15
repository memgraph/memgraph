#!/usr/bin/python3

import base64
import io
import json
import os
import re


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
    config["role_mapping_mode"] = os.environ.get("MEMGRAPH_SSO_KERBEROS_ROLE_MAPPING_MODE", "principal")
    config["ldap_uri"] = os.environ.get("MEMGRAPH_SSO_KERBEROS_LDAP_URI", "")
    config["ldap_base_dn"] = os.environ.get("MEMGRAPH_SSO_KERBEROS_LDAP_BASE_DN", "")
    config["ldap_search_base"] = os.environ.get("MEMGRAPH_SSO_KERBEROS_LDAP_SEARCH_BASE", "")
    # LDAP auth: "gssapi" (default, uses keytab) or "simple" (bind DN + password)
    config["ldap_auth"] = os.environ.get("MEMGRAPH_SSO_KERBEROS_LDAP_AUTH", "gssapi")
    config["ldap_bind_dn"] = os.environ.get("MEMGRAPH_SSO_KERBEROS_LDAP_BIND_DN", "")
    config["ldap_bind_password"] = os.environ.get("MEMGRAPH_SSO_KERBEROS_LDAP_BIND_PASSWORD", "")
    # LDAP attribute that matches the Kerberos principal name (default: sAMAccountName for AD)
    config["ldap_user_attribute"] = os.environ.get("MEMGRAPH_SSO_KERBEROS_LDAP_USER_ATTRIBUTE", "sAMAccountName")
    config["ldap_user_object_class"] = os.environ.get("MEMGRAPH_SSO_KERBEROS_LDAP_USER_OBJECT_CLASS", "user")
    # Custom LDAP user search filter (optional). Use {username} as placeholder.
    # If not set, defaults to: (&(objectClass={object_class})({user_attribute}={username}))
    config["ldap_user_search_filter"] = os.environ.get("MEMGRAPH_SSO_KERBEROS_LDAP_USER_SEARCH_FILTER", "")
    # LDAP attribute containing group memberships (default: memberOf)
    config["ldap_group_membership_attribute"] = os.environ.get(
        "MEMGRAPH_SSO_KERBEROS_LDAP_GROUP_MEMBERSHIP_ATTRIBUTE", "memberOf"
    )
    # Nested groups: when enabled, transitively resolves group membership.
    # Default search filter uses AD's LDAP_MATCHING_RULE_IN_CHAIN (1.2.840.113556.1.4.1941).
    config["ldap_nested_groups_enabled"] = (
        os.environ.get("MEMGRAPH_SSO_KERBEROS_LDAP_NESTED_GROUPS_ENABLED", "false").lower() == "true"
    )
    config["ldap_nested_groups_search_filter"] = os.environ.get(
        "MEMGRAPH_SSO_KERBEROS_LDAP_NESTED_GROUPS_SEARCH_FILTER",
        "(&(objectClass=group)(member:1.2.840.113556.1.4.1941:={user_dn}))",
    )
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
        server = ldap3.Server(ldap_uri, get_info=ldap3.NONE)
        if config["ldap_auth"] == "simple":
            conn = ldap3.Connection(server, user=config["ldap_bind_dn"], password=config["ldap_bind_password"])
        else:
            conn = ldap3.Connection(server, authentication=ldap3.SASL, sasl_mechanism=ldap3.KERBEROS)
        if not conn.bind():
            return {"error": f"LDAP bind failed: {conn.result}"}

        custom_filter = config["ldap_user_search_filter"]
        if custom_filter:
            search_filter = custom_filter.replace("{username}", ldap3.utils.conv.escape_filter_chars(principal_name))
        else:
            user_attr = config["ldap_user_attribute"]
            user_obj_class = config["ldap_user_object_class"]
            search_filter = (
                f"(&(objectClass={user_obj_class})({user_attr}={ldap3.utils.conv.escape_filter_chars(principal_name)}))"
            )
        group_attr = config["ldap_group_membership_attribute"]
        conn.search(search_base, search_filter, attributes=[group_attr])

        if not conn.entries:
            return {"error": f"User {principal_name} not found in LDAP under {search_base}"}

        if config["ldap_nested_groups_enabled"]:
            user_dn = conn.entries[0].entry_dn
            nested_filter = config["ldap_nested_groups_search_filter"].replace(
                "{user_dn}", ldap3.utils.conv.escape_filter_chars(user_dn)
            )
            conn.search(base_dn, nested_filter, attributes=["cn"])
            user_groups = {entry.cn.value for entry in conn.entries if entry.cn}
            member_of = []
        else:
            group_entry = conn.entries[0][group_attr] if group_attr in conn.entries[0] else None
            member_of = group_entry.values if group_entry else []
            user_groups = None
        conn.unbind()
    except Exception as e:
        return {"error": f"LDAP query failed: {str(e)}"}

    # If nested groups path didn't already populate user_groups, parse from memberOf DNs
    if user_groups is None:
        # Extract CN from group DNs using ldap3's parser (handles escaped chars correctly)
        from ldap3.utils.dn import parse_dn

        user_groups = set()
        for group_dn in member_of:
            parsed = parse_dn(group_dn)
            if parsed and parsed[0][0].upper() == "CN":
                # parse_dn returns the value with LDAP escapes still present (e.g. "Engineering\, EMEA").
                # Unescape so it matches what the admin puts in MEMGRAPH_SSO_KERBEROS_ROLE_MAPPING.
                escaped_value = parsed[0][1]
                unescaped = re.sub(r"\\(.)", r"\1", escaped_value)
                user_groups.add(unescaped)

    # Match groups against role mapping
    roles = []
    for mapping_key, mg_roles in role_mapping.items():
        if mapping_key in user_groups or mapping_key == "*":
            roles.extend(mg_roles)

    return {"roles": roles, "user_groups": sorted(user_groups)}


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
        return {"authenticated": False, "errors": "principal cannot be mapped"}

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
