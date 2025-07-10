#!/bin/sh
pip3 install -r ${SRC_PKG}/reddit-processor-requirements.txt -t ${SRC_PKG} && cp -r ${SRC_PKG} ${DEPLOY_PKG}