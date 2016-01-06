#!/bin/bash

GENII_TRUNK="/home/dg/eclipse-workspace/GenesisII-trunk/"

read -p "Are you sure to overwrite files in GenesisII trunk? y/n: " -n 1 -r
echo    # (optional) move to a new line
if [[ $REPLY =~ ^[Yy]$ ]]
then
    cp -v deployments/client-config.xml $GENII_TRUNK/deployments/default/configuration/
    cp -v gffs-structure/dqos-manager $GENII_TRUNK/libraries/gffs-structure/trunk/config/tooldocs/description/
    cp -v gffs-structure/qos-manager $GENII_TRUNK/libraries/gffs-structure/trunk/config/tooldocs/man/
    cp -v gffs-structure/uqos-manager $GENII_TRUNK/libraries/gffs-structure/trunk/config/tooldocs/usage/
    cp -v gffs-structure/MkdirTool.java $GENII_TRUNK/libraries/gffs-structure/trunk/src/edu/virginia/vcgr/genii/client/cmd/tools/
    cp -v gffs-structure/QosManagerTool.java $GENII_TRUNK/libraries/gffs-structure/trunk/src/edu/virginia/vcgr/genii/client/cmd/tools/
    cp -v gffs-structure/CopyMachine.java $GENII_TRUNK/libraries/gffs-structure/trunk/src/edu/virginia/vcgr/genii/client/rns/
fi

