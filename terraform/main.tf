provider "aws" {
    region = "us-east-1"
}

resource "aws_security_group" "aircraft-bdi-isar-sg" {
  name = "aircraft-bdi-isar-security-group"

  # Allow SSH
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Allow HTTP to Docker
  ingress {
    from_port   = 8000
    to_port     = 8000
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Allow communication from EC2 to RDS and vice versa
  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Allow all outbound traffic
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

variable "db_username" {}
variable "db_password" {}

resource "aws_db_instance" "aircraft_bdi_isar_rds" {
    allocated_storage   = 20
    db_name             = "postgres"
    engine              = "postgres"
    engine_version      = "17.2"
    instance_class      = "db.t3.micro"
    username            = var.db_username
    password            = var.db_password
    publicly_accessible = false
    vpc_security_group_ids = [aws_security_group.aircraft-bdi-isar-sg.id]

    tags = {
      Name = "AircraftBDIIsarRDS"
    }
}

resource "aws_instance" "aircraft-bdi-isar-ec2" {
    ami             = "ami-08b5b3a93ed654d19"
    instance_type   = "t2.medium" # this needs to be higher so the instance doesn't stall at the /prepare endpoint
    key_name        = "vockey"
    security_groups = [aws_security_group.aircraft-bdi-isar-sg.name]

    depends_on = [aws_db_instance.aircraft_bdi_isar_rds]

    iam_instance_profile = "LabInstanceProfile"

    user_data = <<-EOF
      #!/bin/bash
      sudo yum update -y
      sudo amazon-linux-extras enable docker
      sudo yum install -y docker
      sudo service docker start
      sudo usermod -aG docker ec2-user
      aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 440355472446.dkr.ecr.us-east-1.amazonaws.com
      docker pull 440355472446.dkr.ecr.us-east-1.amazonaws.com/bdi-app-ecr:latest
      docker run -d -p 8000:8000 \
        -v /root/.aws:/root/.aws \
        -e bdi_db_username=${var.db_username} \
        -e bdi_db_password=${var.db_password} \
        -e bdi_db_host=${split(":", aws_db_instance.aircraft_bdi_isar_rds.endpoint)[0]} \
        -e bdi_db_port=${split(":", aws_db_instance.aircraft_bdi_isar_rds.endpoint)[1]} \
        440355472446.dkr.ecr.us-east-1.amazonaws.com/bdi-app-ecr:latest
    EOF
}

