#!/bin/bash

GENII_TRUNK="/home/dg/eclipse-workspace/GenesisII-trunk/"

cp -v $GENII_TRUNK/deployments/default/configuration/client-config.xml deployments/
cp -v $GENII_TRUNK/libraries/gffs-structure/trunk/config/tooldocs/description/dqos-manager gffs-structure/
cp -v $GENII_TRUNK/libraries/gffs-structure/trunk/config/tooldocs/man/qos-manager gffs-structure/
cp -v $GENII_TRUNK/libraries/gffs-structure/trunk/config/tooldocs/usage/uqos-manager gffs-structure/
cp -v $GENII_TRUNK/libraries/gffs-structure/trunk/src/edu/virginia/vcgr/genii/client/cmd/tools/MkdirTool.java gffs-structure/
cp -v $GENII_TRUNK/libraries/gffs-structure/trunk/src/edu/virginia/vcgr/genii/client/cmd/tools/QosManagerTool.java gffs-structure/
cp -v $GENII_TRUNK/libraries/gffs-structure/trunk/src/edu/virginia/vcgr/genii/client/rns/CopyMachine.java gffs-structure/
