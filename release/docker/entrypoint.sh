#!/bin/bash

MEMGRAPH_USER=${MEMGRAPH_UID:-memgraph}
MEMGRAPH_USER_GROUP=${MEMGRAPH_GID:memgraph}

if [[ -z  ${MEMGRAPH_UID} && -z ${MEMGRAPH_GID} ]]; then
    if [[ "$(id -u)" != "0" ]]; then
        >&2 echo "ERROR: Not running as root. Please run as root, and pass MEMGRAPH_UID and MEMGRAPH_GID."
        >&2 echo "    e.g.: docker run -e MEMGRAPH_UID=\$(id -u) -e MEMGRAPH_GID=\$(id -g) ..."
        exit 10
    fi

    EXISTING_GROUP_NAME=$(getent group ${MEMGRAPH_GID} | cut -d: -f1)
    DEBUG=${DEBUG:no}
    if [ "${EXISTING_GROUP_NAME}" != "" ]; then
        if [ "${DEBUG}" = "yes" ]; then
            >&2 echo "DEBUG: Renaming group '${EXISTING_GROUP_NAME}' (${MEMGRAPH_GID}) to 'usergroup'."
        fi
        delgroup ${EXISTING_GROUP_NAME}
    fi

    # (Re)create the group 'usergroup'
    delgroup usergroup 2>/dev/null || true
    addgroup -g "${MEMGRAPH_GID}" usergroup

    # (Re)create the user 'user'
    deluser user 2>/dev/null || true
    adduser -D -H -u "${MEMGRAPH_UID}" -G usergroup user

    # Determine Docker group ID
    DOCKER_GID=$(stat -c "%g" /var/run/docker.sock)

    # If Docker group exists in container, assign user to that group
    EXISTING_DOCKER_GROUP_NAME=$(getent group ${DOCKER_GID} | cut -d: -f1)
    if [ "${EXISTING_DOCKER_GROUP_NAME}" != "" ]; then
        addgroup user "${EXISTING_DOCKER_GROUP_NAME}"

    # If Docker group does not exist in container, create it and assign user to it
    else
        delgroup docker 2>/dev/null || true
        addgroup -g "${DOCKER_GID}" docker
        addgroup user docker
    fi

fi