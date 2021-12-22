#!/bin/bash
names=$(awk -F "=" '/names/ {print $2}' config.ini)

rm logfile.log

#echo "Generating reports ..."

# python3 updateProtocols.py

#echo "Running dev.py ..."
#for name in $names; do
#    python dev.py $name
#done

#echo "Running contr.py ..."
#for name in $names; do
#    python contr.py protocols/$name.toml 1
#done

for name in $names; do
    python get_repo_stats.py $name 1
done

#echo "Running visualizer ..."
#python vis.py