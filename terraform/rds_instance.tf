resource "aws_db_instance" "capstone" {
  instance_class    = "db.t3.micro"
  allocated_storage = 64
  db_name           = "capstone"
  engine            = "postgres"
  username          = "postgres"
  password          = random_password.master_password.result
  publicly_accessible = true
}