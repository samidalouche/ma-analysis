#!/bin/sh

set -e
cp ./lib/ta-lib-0.4.0.jar ./dist/hackreduce-0.1.jar ship/

cd ship
rm -rf tmp
mkdir tmp
unzip hackreduce-0.1.jar -d tmp
rm hackreduce-0.1.jar
cd tmp
mkdir lib
cp ../ta-lib-0.4.0.jar ./lib
jar cvf ../hackreduce-0.1.jar *
cd ..
rm -rf tmp
cd ..

for i in ship/* ; do
	scp -i $HOME/hackreduce.pem $i hadoop@hackreduce-namenode.hopper.to:/home/hadoop/user/resistor/
done
