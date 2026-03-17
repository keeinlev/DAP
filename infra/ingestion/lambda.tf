resource "aws_lambda_function" "ingestion" {
  function_name = "ingestion-${terraform.workspace}-lambda"
  role          = aws_iam_role.lambda_exec_role.arn
  package_type  = "Image"
  image_uri     = "${var.aws_ecr_repo_url}:15b73" # IMPORTANT: CHANGE THIS TO BE INJECTED BY WORKFLOW

  architectures = ["arm64"] # Graviton support for better price/performance
}