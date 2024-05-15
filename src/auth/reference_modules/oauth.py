#!/usr/bin/python3

import io
import json
import os
import time

import dotenv
import jwt

import requests


def validate_jwt_token(token, tenant_id, client_id):
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


def _load_config_from_env():
    config = {}

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

    return config


def authenticate(response: str, scheme: str):
    if scheme not in ["oauth-entra-id", "oidc-entra-id"]:
        return {"authenticated": False, "error": "Invalid SSO scheme"}

    config = _load_config_from_env()

    tokens = None
    if scheme == "oauth-entra-id":
        tokens = {"access_token": response}
    else:
        tokens = _parse_response(response)

    access_token_validity = validate_jwt_token(tokens["access_token"], config["tenant_id"], config["client_id"])
    if "error" in access_token_validity:
        return {"authenticated": False, "error": access_token_validity["error"]}

    if scheme == "oidc-entra-id":
        id_token_validity = validate_jwt_token(tokens["id_token"], config["tenant_id"], config["client_id"])
        if "error" in id_token_validity:
            return {"authenticated": False, "error": id_token_validity["error"]}

    roles = access_token_validity["token"]["roles"]
    role = roles[0] if isinstance(roles, list) else roles

    if role not in config["role_mapping"]:
        return {"authenticated": False, "error": f"Cannot map role {role} to Memgraph role"}

    return {
        "authenticated": True,
        "role": config["role_mapping"][role],
        "username": access_token_validity["token"]["preferred_username"],
    }


if __name__ == "__main__":
    dotenv.load_dotenv("/home/ivan/work/lab/.env.local")

    input_stream = io.FileIO(1000, mode="r")
    output_stream = io.FileIO(1001, mode="w")
    while True:
        params = json.loads(input_stream.readline().decode("ascii"))
        print("params", params)
        ret = authenticate(**params)
        print("result", ret)
        output_stream.write((json.dumps(ret) + "\n").encode("ascii"))
