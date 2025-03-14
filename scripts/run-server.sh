#!/bin/bash
cd ..

sudo apt-get install -y ca-certificates
sudo cp certs/development-certificate.crt /usr/local/share/ca-certificates
sudo update-ca-certificates

docker compose up --build -d
