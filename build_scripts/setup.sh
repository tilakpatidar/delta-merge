#!/bin/bash
# Install pip
easy_install pip
# Install autoenv to switch env
pip install autoenv
echo "source `which activate.sh`" >> ~/.bashrc

# Install virtualenv
pip install virtualenv
virtualenv env
source env/bin/activate
source .env
curl https://bootstrap.pypa.io/get-pip.py | python
pip install -r requirements.txt

echo "$PWD/scripts/lib_python" > env/lib/python2.7/site-packages/lib_python.pth
echo $SPARK_HOME/python > env/lib/python2.7/site-packages/pyspark.pth

# Install spark2.3
wget http://www-us.apache.org/dist/spark/spark-2.3.1/spark-2.3.1-bin-hadoop2.7.tgz
tar -xvf spark-2.3.1-bin-hadoop2.7.tgz
mv spark-2.3.1-bin-hadoop2.7 spark-2.3.1
rm -rf spark-2.3.1-bin-hadoop2.7.tgz

# Setup log4j properties
cp $SPARK_HOME/conf/log4j.properties.template $SPARK_HOME/conf/log4j.properties
sed -i -e 's/^log4j.rootCategory=INFO, console/log4j.rootCategory=ERROR, console/g' $SPARK_HOME/conf/log4j.properties