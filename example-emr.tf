locals {
  launch_date = "${timestamp()}"
  owner = "example-owner"
}

provider "aws" {
  region = "us-west-1"
}

resource "aws_emr_cluster" "emr-example-cluster" {
  name          = "${local.owner}-emr-example-${local.launch_date}"
  release_label = "emr-5.12.0"
  applications  = ["Spark", "Hadoop", "Hive", "Tez", "Zeppelin", "Hue", "Ganglia"]
  log_uri       = "s3://example-bucket/emr-job-logs-test/"

  termination_protection = false
  keep_job_flow_alive_when_no_steps = true

  ec2_attributes {
    key_name                          = "example-emr-keypair"
    subnet_id                         = "subnet-12345"
    instance_profile                  = "example-ec2-emr-services-role"
    emr_managed_master_security_group = "sg-2345"
    emr_managed_slave_security_group  = "sg-3456"
    service_access_security_group     = "sg-4567"
  }

  instance_group {
      instance_role = "MASTER"
      instance_type = "r4.8xlarge"
      instance_count = "1"
      ebs_config {
        size = "200"
        type = "gp2"
        volumes_per_instance = 1
      }
  }

  instance_group {
      instance_role = "CORE"
      instance_type = "r4.8xlarge"
      instance_count = "4"
      ebs_config {
        size = "1024"
        type = "gp2"
        volumes_per_instance = 1
      }
  }

  tags {
    owner = "${local.owner}"
    source = "terraform"
  }

  configurations = "emr_configurations.json"
  service_role = "example-emr-services-role"
}

/*
# setup a task instance group
resource "aws_emr_instance_group" "emr-example-task" {
  cluster_id     = "${aws_emr_cluster.emr-example-cluster.id}"
  instance_type  = "m4.2xlarge"
  instance_count = 0
  name           = "task"
}
*/

output "master_public_dns" {
  value = "${aws_emr_cluster.emr-example-cluster.master_public_dns}"
}
