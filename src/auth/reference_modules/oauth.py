#!/usr/bin/python3

import io
import json
import os
import time

import jwt

import requests


def validate_jwt_token(token: str, scheme: str, client_id, tenant_id):
    jwks_uri = None
    if scheme == "oauth-entra-id":
        jwks_uri = f"https://login.microsoftonline.com/{tenant_id}/discovery/v2.0/keys"

    jwks = requests.get(jwks_uri).json()

    header = jwt.get_unverified_header(token)
    if "alg" not in header or header["alg"] != "RS256":
        return {"valid": False, "error": "Invalid algorithm in header"}

    kid = header["kid"]

    decoded_token = None
    for jwk in jwks["keys"]:
        if kid == jwk["kid"]:
            try:
                public_key = jwt.algorithms.RSAAlgorithm.from_jwk(json.dumps(jwk))
                decoded_token = jwt.decode(token, key=public_key, algorithms=["RS256"], audience=client_id)
            except Exception as e:
                return {"valid": False, "error": e.args}

    if decoded_token is None:
        return {"valid": False, "error": "Matching kid not found"}

    if "aud" not in decoded_token or decoded_token["aud"] != client_id:
        return {"valid": False, "error": "Invalid audience"}

    if decoded_token.get("exp", None) < int(time.time()):
        return {"valid": False, "error": "Token expired"}

    return {"valid": True, "token": decoded_token}


def _parse_response(response):
    return dict(token.split("=") for token in response.split(";"))


def _load_from_file(path: str):
    with open(path, mode="r") as file:
        return file.read()


def _load_config_from_env(scheme: str):
    config = {}

    if scheme == "oauth-entra-id":
        config["id_issuer"] = os.getenv("AUTH_OAUTH_ENTRA_ID_ISSUER")
        config["authorization_url"] = os.getenv("AUTH_OAUTH_ENTRA_ID_AUTHORIZATION_URL")
        config["token_url"] = os.getenv("AUTH_OAUTH_ENTRA_ID_TOKEN_URL")
        config["user_info_url"] = os.getenv("AUTH_OAUTH_ENTRA_ID_USER_INFO_URL")
        config["client_id"] = os.getenv("AUTH_OAUTH_ENTRA_ID_CLIENT_ID")
        config["client_secret"] = os.getenv("AUTH_OAUTH_ENTRA_ID_CLIENT_SECRET")
        config["callback_url"] = os.getenv("AUTH_OAUTH_ENTRA_ID_CALLBACK_URL")
        config["scope"] = os.getenv("AUTH_OAUTH_ENTRA_ID_SCOPE")
        config["tenant_id"] = os.getenv("AUTH_OAUTH_ENTRA_ID_TENANT_ID")
        config["role_mapping"] = json.loads(_load_from_file(os.getenv("AUTH_OAUTH_ENTRA_ID_ROLE_MAPPING")))
    elif scheme == "oauth-okta":
        config["id_issuer"] = os.getenv("AUTH_OAUTH_OKTA_ISSUER")
        config["authorization_url"] = os.getenv("AUTH_OAUTH_OKTA_AUTHORIZATION_URL")
        config["token_url"] = os.getenv("AUTH_OAUTH_OKTA_TOKEN_URL")
        config["client_id"] = os.getenv("AUTH_OAUTH_OKTA_CLIENT_ID")
        config["client_secret"] = os.getenv("AUTH_OAUTH_OKTA_CLIENT_SECRET")
        config["callback_url"] = os.getenv("AUTH_OAUTH_OKTA_CALLBACK_URL")
        config["scope"] = os.getenv("AUTH_OAUTH_OKTA_SCOPE")

    return config


def authenticate(response: str, scheme: str):
    if scheme not in ["oauth-entra-id", "oidc-entra-id", "oauth-okta"]:
        return {"authenticated": False, "error": "Invalid SSO scheme"}

    config = _load_config_from_env(scheme)

    tokens = None
    if scheme in ["oauth-entra-id", "oauth-okta"]:
        tokens = {"access_token": response}
    else:
        tokens = _parse_response(response)

    access_token_validity = None
    if scheme == "oauth-entra-id":
        access_token_validity = validate_jwt_token(
            tokens["access_token"], scheme, config["client_id"], config["tenant_id"]
        )
    elif scheme == "oauth-okta":
        validation_url = f"{config['id_issuer']}/oauth2/v1/introspect"
        data = {
            "token": tokens["access_token"],
            "token_type_hint": "access_token",
            "client_id": config["client_id"],
            "client_secret": config["client_secret"],
        }
        res = requests.post(validation_url, data=data).json()
        if "active" not in res or res["active"] == False:
            return {"authenticated": False, "error": "Invalid access token"}
        token = jwt.decode(tokens["access_token"], options={"verify_signature": False})
        token["username"] = res["username"]
        access_token_validity = {"valid": True, "token": token}

    if "error" in access_token_validity:
        return {"authenticated": False, "error": access_token_validity["error"]}

    if scheme == "oidc-entra-id":
        id_token_validity = validate_jwt_token(tokens["id_token"], scheme, config["tenant_id"], config["client_id"])
        if "error" in id_token_validity:
            return {"authenticated": False, "error": id_token_validity["error"]}

    token = access_token_validity["token"]
    if "roles" not in token:
        return {
            "authenticated": False,
            "error": "Missing roles field, roles are probably not correctly configured on the token issuer",
        }

    roles = token["roles"]
    role = roles[0] if isinstance(roles, list) else roles

    if role not in config["role_mapping"]:
        return {"authenticated": False, "error": f"Cannot map role {role} to Memgraph role"}

    return {
        "authenticated": True,
        "role": config["role_mapping"][role],
        "username": access_token_validity["token"]["preferred_username"]
        if "entra" in scheme
        else access_token_validity["token"]["username"],
    }


if __name__ == "__main__":
    input_stream = io.FileIO(1000, mode="r")
    output_stream = io.FileIO(1001, mode="w")
    while True:
        params = json.loads(input_stream.readline().decode("ascii"))
        ret = authenticate(**params)
        output_stream.write((json.dumps(ret) + "\n").encode("ascii"))
