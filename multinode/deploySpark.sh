#!/bin/bash   

USER_NAME=$(whoami)

USER_NAME_DC=$USER_NAME docker-compose -f docker-composeSpark.yml up

