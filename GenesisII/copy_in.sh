#!/bin/bash

GENII_TRUNK="/home/dg/eclipse-workspace/GenesisII-trunk/"

read -p "Are you sure to overwrite files in GitHub repository? y/n: " -n 1 -r
echo    # (optional) move to a new line
if [[ $REPLY =~ ^[Yy]$ ]]
then
    cp -v $GENII_TRUNK/deployments/default/configuration/client-config.xml deployments/
    cp -v $GENII_TRUNK/libraries/gffs-structure/trunk/config/tooldocs/description/dqos-manager gffs-structure/
    cp -v $GENII_TRUNK/libraries/gffs-structure/trunk/config/tooldocs/man/qos-manager gffs-structure/
    cp -v $GENII_TRUNK/libraries/gffs-structure/trunk/config/tooldocs/usage/uqos-manager gffs-structure/
    cp -v $GENII_TRUNK/libraries/gffs-structure/trunk/src/edu/virginia/vcgr/genii/client/cmd/tools/MkdirTool.java gffs-structure/
    cp -v $GENII_TRUNK/libraries/gffs-structure/trunk/src/edu/virginia/vcgr/genii/client/cmd/tools/QosManagerTool.java gffs-structure/
    cp -v $GENII_TRUNK/libraries/gffs-structure/trunk/src/edu/virginia/vcgr/genii/client/rns/CopyMachine.java gffs-structure/
fi

