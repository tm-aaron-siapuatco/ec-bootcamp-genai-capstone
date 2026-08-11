# Infra-design-milestone-aaron
This repository contains Infrastructure as Code to deploy an Azure environment (networking + a single Linux VM) using Terraform. The app stack itself (Postgres, ChromaDB, Dagster, backend, frontend) runs as containers on that VM via Docker Compose — there is no managed Azure SQL/database resource in Terraform.

This folder contains the Terraform configuration for the Azure infrastructure.

## Local setup

1. If you already have an SSH keypair, use your public key:

```bash
cat ~/.ssh/id_rsa.pub
```

If you do not have an SSH keypair yet, create one:

```bash
ssh-keygen -t ed25519 -f ~/.ssh/id_ed25519
```

or, if you need RSA:

```bash
ssh-keygen -t rsa -b 2048 -f ~/.ssh/id_rsa
```

2. Copy the example file and fill in your secrets:

```bash
cp infra/terraform.tfvars.example infra/terraform.tfvars
```

3. Edit `infra/terraform.tfvars` and set:
- `ssh_public_key` to the full one-line public key string from `~/.ssh/id_rsa.pub` or `~/.ssh/id_ed25519.pub`

4. Run Terraform from the `infra/` folder:

```bash
cd infra
terraform init
terraform plan -out=tfplan
terraform apply tfplan
```

The VM boots with Docker + the Compose plugin pre-installed via `scripts/init.sh` (cloud-init). No app code or secrets are baked into the image.

## Deploy the containers

There is no manual deploy script — deployment happens through GitHub Actions (`.github/workflows/ci.yml`), triggered on every push to `main`:

1. `build-and-push` builds the `pipelines` (dagster), `backend`, and `frontend` images and pushes them to GHCR.
2. `deploy` copies `docker-compose.yml` and the `pipelines/` directory to the VM, writes a `.env` from repo secrets, then runs `docker compose pull && docker compose up -d --remove-orphans` over SSH.

To redeploy without a code change, re-run the workflow from the Actions tab (`gh workflow run ci.yml`).

## Verify the deployment

1. SSH into the VM
```bash
ssh -i ~/.ssh/id_rsa azureuser@<vm-public-ip>
```

```bash
docker compose ps
docker compose logs -f

# Verify Port Connectivity & Access the Web UI
curl -I http://localhost:3000                    # dagster-webserver
curl -I http://localhost:8000/api/v1/heartbeat    # chromadb
curl -I http://localhost:8001                     # backend
curl -I http://localhost:8501                     # frontend
```

If everything is working correctly the appropriate dev sites should load. Note these ports are only reachable from inside the VM (over SSH) — the NSG only opens 22/80/443, so there's no direct external access to 3000/8000/8001/8501.

## CI/CD setup

The GitHub workflow (`.github/workflows/ci.yml`) authenticates to the VM over SSH — it never talks to the Azure API directly, so no Azure service-principal secrets are needed.

Set these secrets in GitHub:
- `VM_HOST` — the VM's public IP
- `VM_USERNAME` — `azureuser`
- `VM_SSH_KEY` — the **private** key content (e.g. `cat ~/.ssh/id_ed25519`), matching the public key you set as `ssh_public_key` in `terraform.tfvars`. GitHub Actions uses this to SSH into the VM as a client, so it needs the private half, not `.pub`.
- `AZURE_OPENAI_API_KEY`
- `AZURE_OPENAI_API_ENDPOINT`
- `AZURE_OPENAI_API_VERSION`
- `AZURE_OPENAI_API_CHAT_DEPLOYMENT`
- `EMBEDDING_MODEL`
- `DATABASE_HOST`
- `DATABASE_NAME`
- `DATABASE_USER`
- `DATABASE_PASSWORD`
- `DATABASE_PORT`

## Notes

- Do not commit `infra/terraform.tfvars`.
- `ssh_public_key` is the public key file; `VM_SSH_KEY` (the GitHub secret) is the private key content — don't mix these up.
- SSH (port 22) is open to `0.0.0.0/0` in the NSG. This is intentional, not an oversight: the `deploy` job in `ci.yml` SSHes into the VM from GitHub-hosted Actions runners, which come from a large, constantly-rotating IP pool with no stable range to scope the NSG to. Scoping `Allow-SSH` to a single IP was tried and breaks CI (the runner's IP is never your IP). The actual access control is key-only auth (`disable_password_authentication = true` in `main.tf`) — there is no password to brute-force.