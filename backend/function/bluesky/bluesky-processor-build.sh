#!/bin/sh
pip3 install -r ${SRC_PKG}/bluesky-processor-requirements.txt -t ${SRC_PKG} && cp -r ${SRC_PKG} ${DEPLOY_PKG}