#!/usr/bin/python3

import io
import json
import os
import time

import jwt

import requests


def validate_jwt_token(token: str, scheme: str, config: dict, token_type: str):
    jwks_uri = None
    if scheme == "oauth-entra-id":
        jwks_uri = f"https://login.microsoftonline.com/{config['tenant_id']}/discovery/v2.0/keys"
    elif scheme == "oauth-okta":
        jwks_uri = f"{config['id_issuer']}/v1/keys"

    jwks = requests.get(jwks_uri).json()

    # need the header to match KID with provider
    header = jwt.get_unverified_header(token)  # NOSONAR
    if "alg" not in header or header["alg"] != "RS256":
        return {"valid": False, "error": "Invalid algorithm in header"}

    kid = header["kid"]

    decoded_token = None
    for jwk in jwks["keys"]:
        if kid == jwk["kid"]:
            try:
                public_key = jwt.algorithms.RSAAlgorithm.from_jwk(json.dumps(jwk))
                if scheme == "oauth-okta" and token_type == "access":
                    decoded_token = jwt.decode(
                        token, key=public_key, algorithms=["RS256"], audience=config["authorization_server"]
                    )
                else:
                    decoded_token = jwt.decode(
                        token, key=public_key, algorithms=["RS256"], audience=config["client_id"]
                    )
            except Exception as e:
                return {"valid": False, "error": e.args}

    if decoded_token is None:
        return {"valid": False, "error": "Matching kid not found"}

    if decoded_token.get("exp", None) < int(time.time()):
        return {"valid": False, "error": "Token expired"}

    return {"valid": True, "token": decoded_token}


def _parse_response(response):
    return dict(token.split("=") for token in response.split(";"))


def _load_role_mappings(raw_role_mappings: str) -> dict:
    if raw_role_mappings:
        role_mapping = {}
        raw_role_mappings = raw_role_mappings.split(";")
        for mapping in raw_role_mappings:
            idp_role, mg_role = mapping.split(":")
            role_mapping[idp_role.strip()] = mg_role.strip()
        return role_mapping
    return {}


def _load_config_from_env(scheme: str):
    config = {}

    if scheme == "oauth-entra-id":
        config["client_id"] = os.environ.get("MEMGRAPH_SSO_ENTRA_ID_OAUTH_CLIENT_ID", "")
        config["tenant_id"] = os.environ.get("MEMGRAPH_SSO_ENTRA_ID_OAUTH_TENANT_ID", "")
        config["role_mapping"] = _load_role_mappings(os.environ.get("MEMGRAPH_SSO_ENTRA_ID_OAUTH_ROLE_MAPPING", ""))

    elif scheme == "oauth-okta":
        config["client_id"] = os.environ.get("MEMGRAPH_SSO_OKTA_OAUTH_CLIENT_ID", "")
        config["id_issuer"] = os.environ.get("MEMGRAPH_SSO_OKTA_OAUTH_ISSUER", "")
        config["authorization_server"] = os.environ.get("MEMGRAPH_SSO_OKTA_AUTHORIZATION_SERVER", "")
        config["role_mapping"] = _load_role_mappings(os.environ.get("MEMGRAPH_SSO_OKTA_OAUTH_ROLE_MAPPING", ""))

    return config


def decode_tokens(scheme, config, tokens):
    return (
        validate_jwt_token(tokens["access_token"], scheme, config, "access"),
        validate_jwt_token(tokens["id_token"], scheme, config, "id"),
    )


def process_tokens(tokens, config):
    access_token, id_token = tokens
    if "error" in access_token:
        return {"authenticated": False, "errors": access_token["error"]}
    if "error" in id_token:
        return {"authenticated": False, "errors": id_token["error"]}

    access_token = access_token["token"]
    id_token = id_token["token"]
    if "roles" not in access_token:
        return {
            "authenticated": False,
            "errors": "Missing roles field, roles are probably not correctly configured on the token issuer",
        }

    roles = access_token["roles"]
    role = roles[0] if isinstance(roles, list) else roles

    if role not in config["role_mapping"]:
        return {"authenticated": False, "errors": f"Cannot map role {role} to Memgraph role"}

    return {
        "authenticated": True,
        "role": config["role_mapping"][role],
        "username": id_token["sub"],
    }


def authenticate(response: str, scheme: str):
    if scheme not in ["oauth-entra-id", "oauth-okta"]:
        return {"authenticated": False, "error": "Invalid SSO scheme"}

    config = _load_config_from_env(scheme)
    tokens = _parse_response(response)

    return process_tokens(decode_tokens(scheme, config, tokens), config, scheme)


if __name__ == "__main__":
    input_stream = io.FileIO(1000, mode="r")
    output_stream = io.FileIO(1001, mode="w")
    while True:
        params = json.loads(input_stream.readline().decode("ascii"))
        ret = authenticate(**params)
        output_stream.write((json.dumps(ret) + "\n").encode("ascii"))
