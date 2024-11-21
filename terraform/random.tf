resource "random_password" "master_password" {
  length           = 16
  special          = false
}
