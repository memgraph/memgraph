#!/usr/bin/python3

import io
import json
import os
import time

import jwt

import requests


def validate_jwt_token(token: str, scheme: str, config: dict, token_type: str):
    jwks_uri = None
    if scheme == "oidc-entra-id":
        jwks_uri = f"https://login.microsoftonline.com/{config['tenant_id']}/discovery/v2.0/keys"
    elif scheme == "oidc-okta":
        jwks_uri = f"{config['id_issuer']}/v1/keys"
    elif scheme == "oidc-custom":
        jwks_uri = f"{config['public_key_endpoint']}"

    try:
        response = requests.get(jwks_uri, timeout=10, verify=config["cert"])
        response.raise_for_status()
        jwks = response.json()
    except (requests.RequestException, ValueError) as e:
        return {"valid": False, "errors": f"Failed to fetch JWKS: {str(e)}"}

    # need the header to match KID with provider
    try:
        header = jwt.get_unverified_header(token)  # NOSONAR
    except Exception as e:
        return {"valid": False, "errors": f"Failed to decode JWT header: {str(e)}"}

    if "alg" not in header or header["alg"] != "RS256":
        return {"valid": False, "errors": "Invalid algorithm in header"}

    if "kid" not in header:
        return {"valid": False, "errors": "Missing key ID (kid) in JWT header"}

    kid = header["kid"]

    if "keys" not in jwks or not isinstance(jwks["keys"], list):
        return {"valid": False, "errors": "Invalid JWKS response: missing or invalid keys array"}

    decoded_token = None
    for jwk in jwks["keys"]:
        if kid != jwk["kid"]:
            continue
        try:
            public_key = jwt.algorithms.RSAAlgorithm.from_jwk(json.dumps(jwk))
            if scheme == "oidc-custom" and token_type == "access":
                decoded_token = jwt.decode(
                    token, key=public_key, algorithms=["RS256"], audience=config["access_token_audience"]
                )
            elif scheme == "oidc-custom" and token_type == "id":
                decoded_token = jwt.decode(
                    token, key=public_key, algorithms=["RS256"], audience=config["id_token_audience"]
                )
            elif scheme == "oidc-okta" and token_type == "access":
                decoded_token = jwt.decode(
                    token, key=public_key, algorithms=["RS256"], audience=config["authorization_server"]
                )
            else:
                decoded_token = jwt.decode(token, key=public_key, algorithms=["RS256"], audience=config["client_id"])
        except Exception as e:
            return {"valid": False, "errors": " ".join(e.args)}

    if decoded_token is None:
        return {"valid": False, "errors": "Matching kid not found"}

    exp = decoded_token.get("exp")
    if exp is None:
        return {"valid": False, "errors": "Token missing expiration claim"}

    try:
        if int(exp) < int(time.time()):
            return {"valid": False, "errors": "Token expired"}
    except (ValueError, TypeError):
        return {"valid": False, "errors": "Invalid expiration claim in token"}

    return {"valid": True, "token": decoded_token}


def _parse_response(response):
    return dict(token.split("=") for token in response.split(";"))


def _load_role_mappings(raw_role_mappings: str) -> dict:
    if raw_role_mappings:
        role_mapping = {}
        raw_role_mappings = raw_role_mappings.strip().split(";")
        for mapping in raw_role_mappings:
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


def _load_config_from_env(scheme: str):
    config = {}

    if scheme == "oidc-entra-id":
        config["client_id"] = os.environ.get("MEMGRAPH_SSO_ENTRA_ID_OIDC_CLIENT_ID", "")
        config["tenant_id"] = os.environ.get("MEMGRAPH_SSO_ENTRA_ID_OIDC_TENANT_ID", "")
        config["role_field"] = os.environ.get("MEMGRAPH_SSO_ENTRA_ID_OIDC_ROLE_FIELD", "roles")
        config["username"] = os.environ.get("MEMGRAPH_SSO_ENTRA_ID_OIDC_USERNAME", "id:sub")
        config["role_mapping"] = _load_role_mappings(os.environ.get("MEMGRAPH_SSO_ENTRA_ID_OIDC_ROLE_MAPPING", ""))
        # by default check the certificate, this is also the default behavior of the requests library
        config["cert"] = os.environ.get("MEMGRAPH_SSO_ENTRA_ID_OIDC_EXTRA_CA_CERTS", True)
    elif scheme == "oidc-okta":
        config["client_id"] = os.environ.get("MEMGRAPH_SSO_OKTA_OIDC_CLIENT_ID", "")
        config["id_issuer"] = os.environ.get("MEMGRAPH_SSO_OKTA_OIDC_ISSUER", "")
        config["authorization_server"] = os.environ.get("MEMGRAPH_SSO_OKTA_OIDC_AUTHORIZATION_SERVER", "")
        config["role_field"] = os.environ.get("MEMGRAPH_SSO_OKTA_OIDC_ROLE_FIELD", "groups")
        config["username"] = os.environ.get("MEMGRAPH_SSO_OKTA_OIDC_USERNAME", "id:sub")
        config["role_mapping"] = _load_role_mappings(os.environ.get("MEMGRAPH_SSO_OKTA_OIDC_ROLE_MAPPING", ""))
        config["cert"] = os.environ.get("MEMGRAPH_SSO_OKTA_OIDC_EXTRA_CA_CERTS", True)
    elif scheme == "oidc-custom":
        config["public_key_endpoint"] = os.environ.get("MEMGRAPH_SSO_CUSTOM_OIDC_PUBLIC_KEY_ENDPOINT", "")
        config["access_token_audience"] = os.environ.get("MEMGRAPH_SSO_CUSTOM_OIDC_ACCESS_TOKEN_AUDIENCE", "")
        config["id_token_audience"] = os.environ.get("MEMGRAPH_SSO_CUSTOM_OIDC_ID_TOKEN_AUDIENCE", "")
        config["role_field"] = os.environ.get("MEMGRAPH_SSO_CUSTOM_OIDC_ROLE_FIELD", "")
        config["username"] = os.environ.get("MEMGRAPH_SSO_CUSTOM_OIDC_USERNAME", "")
        config["role_mapping"] = _load_role_mappings(os.environ.get("MEMGRAPH_SSO_CUSTOM_OIDC_ROLE_MAPPING", ""))
        config["cert"] = os.environ.get("MEMGRAPH_SSO_CUSTOM_OIDC_EXTRA_CA_CERTS", True)
    # if ID token is not used it won't be checked
    config["use_id_token"] = config["username"].startswith("id:")
    return config


def decode_tokens(scheme: str, config: dict, tokens: dict):
    if config["use_id_token"]:
        return (
            validate_jwt_token(tokens["access_token"], scheme, config, "access"),
            validate_jwt_token(tokens["id_token"], scheme, config, "id"),
        )

    return (validate_jwt_token(tokens["access_token"], scheme, config, "access"),)


def process_tokens(tokens: tuple, config: dict, scheme: str):
    access_token = tokens[0]
    id_token = tokens[1] if config["use_id_token"] else None

    if "errors" in access_token:
        return {"authenticated": False, "errors": f"Error while decoding access token: {access_token['errors']}"}
    if id_token is not None and "errors" in id_token:
        return {"authenticated": False, "errors": f"Error while decoding id token: {id_token['errors']}"}

    access_token = access_token["token"]
    id_token = id_token["token"] if id_token is not None else None
    roles_field = config["role_field"]

    if roles_field not in access_token:
        return {
            "authenticated": False,
            "errors": f"Missing roles field named {roles_field}, roles are probably not correctly configured on the token issuer",
        }

    roles = []
    idp_roles = access_token[roles_field]
    if isinstance(idp_roles, list):
        matching_roles = set()

        for idp_role in idp_roles:
            if idp_role in config["role_mapping"]:
                matching_roles.update(config["role_mapping"][idp_role])

        if not matching_roles:
            return {
                "authenticated": False,
                "errors": f"Cannot map any of the roles {sorted(idp_roles)} to Memgraph roles",
            }
        roles = list(matching_roles)
    elif isinstance(idp_roles, str):
        if idp_roles not in config["role_mapping"]:
            return {"authenticated": False, "errors": f"Cannot map role {idp_roles} to Memgraph role"}
        roles = config["role_mapping"][idp_roles]

    try:
        token_type, field = config["username"].split(":")
    except ValueError:
        return {
            "authenticated": False,
            "errors": f"Invalid username configuration format: {config['username']}. Expected format: 'token_type:field_name'",
        }

    if (token_type == "id" and field not in id_token) or (token_type == "access" and field not in access_token):
        return {"authenticated": False, "errors": f"Field {field} missing in {token_type} token"}

    return {
        "authenticated": True,
        "roles": roles,
        "username": id_token[field] if token_type == "id" else access_token[field],
    }


def authenticate(response: str, scheme: str):
    if scheme not in ["oidc-entra-id", "oidc-okta", "oidc-custom"]:
        return {"authenticated": False, "errors": "Invalid SSO scheme"}

    try:
        config = _load_config_from_env(scheme)
        tokens = _parse_response(response)
        return process_tokens(decode_tokens(scheme, config, tokens), config, scheme)
    except Exception as e:
        return {"authenticated": False, "errors": f"Error: {' '.join(e.args)}"}


if __name__ == "__main__":
    input_stream = io.FileIO(1000, mode="r")
    output_stream = io.FileIO(1001, mode="w")
    while True:
        params = json.loads(input_stream.readline().decode("ascii"))
        ret = authenticate(**params)
        output_stream.write((json.dumps(ret) + "\n").encode("ascii"))
