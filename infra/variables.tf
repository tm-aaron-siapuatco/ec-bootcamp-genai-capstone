variable location {
    type = string
    default = "Southeast Asia"
}

variable name {
    type = string
    default = "capstone-aaron"
}

variable "ssh_public_key" {
  type    = string
  description = "The actual public key string, not the file path."
}

variable "allowed_ssh_cidr" {
  type        = string
  description = "CIDR allowed to SSH into the VM, \"<your-ip>/32\".Find yours with `curl -s ifconfig.me`."

  validation {
    condition     = var.allowed_ssh_cidr != "*" && var.allowed_ssh_cidr != "0.0.0.0/0"
    error_message = "allowed_ssh_cidr must not be a wildcard/open range; scope it to your IP or a /32."
  }
}