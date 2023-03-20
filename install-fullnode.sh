#!/bin/bash

# run with
# ssh8 "bash -s" < ./install-fullnode.sh

# install docker https://www.digitalocean.com/community/tutorials/how-to-install-and-use-docker-on-ubuntu-20-04

sudo apt update -y
sudo apt install apt-transport-https ca-certificates curl software-properties-common emacs jq unzip -y
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu focal stable" -y
sudo apt install docker-ce -y
sudo systemctl status docker

# install docker-compose https://www.digitalocean.com/community/tutorials/how-to-install-and-use-docker-compose-on-ubuntu-20-04

sudo curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
docker-compose --version

# install fullnode https://github.com/MystenLabs/sui/tree/main/docker/fullnode

wget https://raw.githubusercontent.com/MystenLabs/sui/main/docker/fullnode/docker-compose.yaml
wget https://raw.githubusercontent.com/MystenLabs/sui/devnet-0.27.1/crates/sui-config/data/fullnode-template.yaml -O ./fullnode-template.yaml
#wget https://github.com/MystenLabs/sui/raw/main/crates/sui-config/data/fullnode-template.yaml
wget https://github.com/MystenLabs/sui-genesis/raw/main/devnet/genesis.blob

# start detached

#sudo docker-compose up -d