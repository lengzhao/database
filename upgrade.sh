#!/bin/bash

./database -c stop
./database -c uninstall

git reset --hard HEAD
git pull
go build
