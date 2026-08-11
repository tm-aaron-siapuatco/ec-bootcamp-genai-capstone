#!/bin/bash
# Install Docker Engine + Compose v2 plugin
curl -fsSL https://get.docker.com | sh

# Start and enable Docker to run on boot
systemctl start docker
systemctl enable docker

# Add your VM admin user to the docker group so SSH commands work without 'sudo'
usermod -aG docker azureuser