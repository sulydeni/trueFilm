set +e

#install dependencies
sudo yum install docker -y
sudo service docker restart
sudo groupadd docker || true
sudo usermod -aG docker $USER

newgrp docker < exit


#run postgres docker
docker pull postgres;
mkdir -p /opt/docker/volumes/postgres;
docker run --rm --name docker -e POSTGRES_PASSWORD=docker -d -p 5432:5432 -v /opt/docker/volumes/postgres:/var/lib/postgresql/data  postgres;

#download & unzip large work files
#downlod the file if it does not exist
FILE=enwiki-latest-abstract.xml
if [ -f "$FILE" ]; then
    echo "$FILE exists"
else 
    wget https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-abstract.xml.gz;
    gunzip enwiki-latest-abstract.xml.gz;
fi


#build pyspark python environment and execution
docker build . -t pyspark

#if test parameter exists run jupyter else execture the script
if [[ $1 == "test" ]]
then
  docker run --network host -v `pwd`:/opt/code pyspark #run jupyter for testing
else
  docker run --network host -v `pwd`:/opt/code -w /opt/code pyspark /usr/bin/python3 main.py #execture the code
fi