resource "aws_ecr_repository" "ingestion_lambda" {
  name = "ingestion-lambda-${terraform.workspace}"
  image_tag_mutability = "IMMUTABLE_WITH_EXCLUSION"

  image_tag_mutability_exclusion_filter {
    filter      = "latest*"
    filter_type = "WILDCARD"
  }
}
