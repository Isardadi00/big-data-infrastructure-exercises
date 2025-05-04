provider "aws" {
    region = "us-east-1"
}

resource "aws_security_group" "aircraft-bdi-isar-api-sg" {
  name = "aircraft-bdi-isar-ec2-security-group"

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

  # Allow all outbound traffic
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "aircraft-bdi-isar-airflow-sg" {
  name = "aircraft-bdi-isar-airflow-security-group"

  # SSH
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Optional: Web UI for Airflow (port 8080)
  ingress {
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "aircraft-bdi-isar-rds-sg" {
  name = "aircraft-bdi-isar-rds-security-group"

  # Allow communication from EC2 to RDS
  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    security_groups = [
      aws_security_group.aircraft-bdi-isar-api-sg.id,
      aws_security_group.aircraft-bdi-isar-airflow-sg.id]
  }

  # Restrict outbound traffic to only PostgreSQL port
  egress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
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
    vpc_security_group_ids = [aws_security_group.aircraft-bdi-isar-rds-sg.id]

    tags = {
      Name = "AircraftBDIIsarRDS"
    }
}

resource "aws_instance" "aircraft-bdi-isar-api-ec2" {
    ami           = "ami-08b5b3a93ed654d19"
    instance_type = "t2.medium"
    key_name      = "vockey"
    vpc_security_group_ids = [aws_security_group.aircraft-bdi-isar-api-sg.id]

    depends_on = [aws_db_instance.aircraft_bdi_isar_rds]

    iam_instance_profile = "LabInstanceProfile"

    tags = {
      Name = "isar-bdi-api"
    }

    user_data = <<-EOF
      #!/bin/bash
      sudo yum update -y
      sudo amazon-linux-extras enable docker
      sudo yum install -y docker
      sudo service docker start
      sudo usermod -aG docker ec2-user

      aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 440355472446.dkr.ecr.us-east-1.amazonaws.com
      docker pull 440355472446.dkr.ecr.us-east-1.amazonaws.com/bdi-api-ecr:latest

      docker run -d -p 8000:8000 \
        -v /root/.aws:/root/.aws \
        -e bdi_db_username=${var.db_username} \
        -e bdi_db_password=${var.db_password} \
        -e bdi_db_host=${aws_db_instance.aircraft_bdi_isar_rds.address} \
        -e bdi_db_port=${aws_db_instance.aircraft_bdi_isar_rds.port} \
        440355472446.dkr.ecr.us-east-1.amazonaws.com/bdi-api-ecr:latest
    EOF
}

resource "aws_instance" "aircraft-bdi-isar-airflow-ec2" {
  ami                    = "ami-08b5b3a93ed654d19"
  instance_type          = "t2.medium"
  key_name               = "vockey"
  vpc_security_group_ids = [aws_security_group.aircraft-bdi-isar-airflow-sg.id]

  iam_instance_profile = "LabInstanceProfile"

  tags = {
    Name = "isar-bdi-airflow"
  }

  user_data = <<-EOF
    #!/bin/bash
    sudo yum update -y
    sudo amazon-linux-extras enable docker
    sudo yum install -y docker
    sudo service docker start
    sudo usermod -aG docker ec2-user

    aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 440355472446.dkr.ecr.us-east-1.amazonaws.com
    docker pull 440355472446.dkr.ecr.us-east-1.amazonaws.com/bdi-airflow-ecr:latest

    # Run the Airflow container
    docker run -d -p 8080:8080 \
      -v /home/ec2-user/.aws:/root/.aws \
      -e S3_BUCKET='aircraft-bdi-isar' \
      -e S3_URL='s3://bdi-aircraft-bucket-isar/' \
      -e S3_KEY_FUEL_CONSUMPTION='fuel_consumption/aircraft_type_fuel_consumption_rates.json' \
      -e DB_USERNAME=${var.db_username} \
      -e DB_PASSWORD=${var.db_password} \
      -e DB_HOST=${aws_db_instance.aircraft_bdi_isar_rds.address} \
      -e DB_PORT=${aws_db_instance.aircraft_bdi_isar_rds.port} \
      --name airflow \
      440355472446.dkr.ecr.us-east-1.amazonaws.com/bdi-airflow-ecr:latest
  EOF
}

