terraform {
    backend "azurerm" {
        resource_group_name = "rg-capstone-backend-aaron"
        storage_account_name = "sttfstatecapstone10571"
        container_name = "tfstate"
        key = "capstone-terraform.tfstate"
    }
}