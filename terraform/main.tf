provider "aws" {
    region = "us-east-1"
}

resource "aws_security_group" "aircraft-bdi-isar-sg" {
  name = "aircraft-bdi-isar-security-group"

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    from_port   = 8000
    to_port     = 8000
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

resource "aws_instance" "aircraft-bdi-isar-ec2" {

    ami             = "ami-04b4f1a9cf54c11d0"
    instance_type   = "t2.micro"
    key_name        = "vockey"
    security_groups = [aws_security_group.aircraft-bdi-isar-sg.name]
}