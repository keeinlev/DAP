resource "aws_lambda_function" "ingestion" {
  count = var.deploy_lambda ? 1 : 0
  function_name = "ingestion-${terraform.workspace}-lambda"
  role          = aws_iam_role.lambda_ecr_role.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.ingestion_lambda.repository_url}:latest"

  architectures = ["arm64"] # Graviton support for better price/performance
}