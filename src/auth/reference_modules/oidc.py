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
    jwks = requests.get(jwks_uri).json()

    # need the header to match KID with provider
    header = jwt.get_unverified_header(token)  # NOSONAR
    if "alg" not in header or header["alg"] != "RS256":
        return {"valid": False, "errors": "Invalid algorithm in header"}

    kid = header["kid"]

    decoded_token = None
    for jwk in jwks["keys"]:
        if kid != jwk["kid"]:
            continue
        try:
            public_key = jwt.algorithms.RSAAlgorithm.from_jwk(json.dumps(jwk))
            if scheme == "oidc-okta" and token_type == "access":
                decoded_token = jwt.decode(
                    token, key=public_key, algorithms=["RS256"], audience=config["authorization_server"]
                )
            else:
                decoded_token = jwt.decode(token, key=public_key, algorithms=["RS256"], audience=config["client_id"])
        except Exception as e:
            return {"valid": False, "errors": " ".join(e.args)}

    if decoded_token is None:
        return {"valid": False, "errors": "Matching kid not found"}

    if decoded_token.get("exp", None) < int(time.time()):
        return {"valid": False, "errors": "Token expired"}

    return {"valid": True, "token": decoded_token}


def _parse_response(response):
    return dict(token.split("=") for token in response.split(";"))


def _load_role_mappings(raw_role_mappings: str) -> dict:
    if raw_role_mappings:
        role_mapping = {}
        raw_role_mappings = raw_role_mappings.strip().split(";")
        for mapping in raw_role_mappings:
            mapping_list = mapping.split(":")
            if len(mapping_list) == 0:
                continue
            if len(mapping_list) != 2:
                raise ValueError(f"Invalid role mapping: {mapping}")
            idp_role, mg_role = mapping_list
            role_mapping[idp_role.strip()] = mg_role.strip()
        return role_mapping

    raise ValueError("Missing role mappings")


def _load_config_from_env(scheme: str):
    config = {}

    if scheme == "oidc-entra-id":
        config["client_id"] = os.environ.get("MEMGRAPH_SSO_ENTRA_ID_OIDC_CLIENT_ID", "")
        config["tenant_id"] = os.environ.get("MEMGRAPH_SSO_ENTRA_ID_OIDC_TENANT_ID", "")
        config["username"] = os.environ.get("MEMGRAPH_SSO_ENTRA_ID_OIDC_USERNAME", "id:sub")
        config["role_mapping"] = _load_role_mappings(os.environ.get("MEMGRAPH_SSO_ENTRA_ID_OIDC_ROLE_MAPPING", {}))

    elif scheme == "oidc-okta":
        config["client_id"] = os.environ.get("MEMGRAPH_SSO_OKTA_OIDC_CLIENT_ID", "")
        config["id_issuer"] = os.environ.get("MEMGRAPH_SSO_OKTA_OIDC_ISSUER", "")
        config["authorization_server"] = os.environ.get("MEMGRAPH_SSO_OKTA_OIDC_AUTHORIZATION_SERVER", "")
        config["username"] = os.environ.get("MEMGRAPH_SSO_OKTA_OIDC_USERNAME", "id:sub")
        config["role_mapping"] = _load_role_mappings(os.environ.get("MEMGRAPH_SSO_OKTA_OIDC_ROLE_MAPPING", {}))

    return config


def decode_tokens(scheme: str, config: dict, tokens: dict):
    return (
        validate_jwt_token(tokens["access_token"], scheme, config, "access"),
        validate_jwt_token(tokens["id_token"], scheme, config, "id"),
    )


def process_tokens(tokens: tuple, config: dict, scheme: str):
    access_token, id_token = tokens

    if "errors" in access_token:
        return {"authenticated": False, "errors": f"Error while decoding access token: {access_token['errors']}"}
    if "errors" in id_token:
        return {"authenticated": False, "errors": f"Error while decoding id token: {id_token['errors']}"}

    access_token = access_token["token"]
    id_token = id_token["token"]
    roles_field = "roles" if scheme == "oidc-entra-id" else "groups"
    if roles_field not in access_token:
        return {
            "authenticated": False,
            "errors": f"Missing roles field named {roles_field}, roles are probably not correctly configured on the token issuer",
        }

    roles = access_token["roles"] if scheme == "oidc-entra-id" else access_token["groups"]
    role = roles[0] if isinstance(roles, list) else roles

    if role not in config["role_mapping"]:
        return {"authenticated": False, "errors": f"Cannot map role {role} to Memgraph role"}

    token_type, field = config["username"].split(":")
    if (token_type == "id" and field not in id_token) or (token_type == "access" and field not in access_token):
        return {"authenticated": False, "errors": f"Field {field} missing in {token_type} token"}

    return {
        "authenticated": True,
        "role": config["role_mapping"][role],
        "username": id_token[field] if token_type == "id" else access_token[field],
    }


def authenticate(response: str, scheme: str):
    if scheme not in ["oidc-entra-id", "oidc-okta"]:
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
