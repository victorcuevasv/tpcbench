#!/bin/bash

#Delete dangling containers.
docker rmi $(docker images --filter "dangling=true" -q --no-trunc)

docker rmi $(docker images --filter=reference="*:dev" -q)

