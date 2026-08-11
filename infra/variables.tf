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