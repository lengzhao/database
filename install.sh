#!/bin/bash

./database -c stop
./database -c uninstall
./database -c install
./database -c start
./database -c stat
echo "Enter to finish"
read a
