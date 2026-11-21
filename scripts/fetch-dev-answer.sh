#!/bin/sh
set -eu
mkdir -p data
wget -q https://www.joinbuilders.org/buildathon/tracks/shopping-sessions/files/dev-answer.zip -O data/dev-answer.zip
unzip -oq data/dev-answer.zip -d data
rm data/dev-answer.zip
