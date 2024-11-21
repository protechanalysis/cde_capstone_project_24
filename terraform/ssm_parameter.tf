resource "aws_ssm_parameter" "master_random" {
  name  = "/testing/database/password/master_password"
  type  = "SecureString"
  value = random_password.master_password.result
}