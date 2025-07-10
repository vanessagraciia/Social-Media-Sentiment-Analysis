#!/bin/sh
pip3 install -r ${SRC_PKG}/reddit-harvester-requirements.txt -t ${SRC_PKG} && cp -r ${SRC_PKG} ${DEPLOY_PKG} # ensure packages are installed