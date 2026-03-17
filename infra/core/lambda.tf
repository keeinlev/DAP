resource "aws_lambda_function" "ingestion" {
  count = var.deploy_lambda ? 1 : 0
  function_name = "ingestion-${terraform.workspace}-lambda"
  role          = aws_iam_role.lambda_exec_role.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.ingestion_lambda.repository_url}:4cd68" # IMPORTANT: CHANGE THIS TO BE INJECTED BY WORKFLOW

  architectures = ["arm64"] # Graviton support for better price/performance
}