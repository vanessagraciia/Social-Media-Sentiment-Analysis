#!/bin/sh
pip3 install -r ${SRC_PKG}/bluesky-harvester-2-requirements.txt -t ${SRC_PKG} && cp -r ${SRC_PKG} ${DEPLOY_PKG}