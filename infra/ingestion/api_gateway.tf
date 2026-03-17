resource "aws_api_gateway_rest_api" "ingestion_api" {
  name = "dap-${terraform.workspace}-ingestion-api"
  description = "API Gateway for DAP Event Ingestion via Lambda"

  endpoint_configuration {
    types = ["REGIONAL"]
  }
}

# Define the root resource

resource "aws_api_gateway_resource" "root" {
  rest_api_id = aws_api_gateway_rest_api.ingestion_api.id
  parent_id = aws_api_gateway_rest_api.ingestion_api.root_resource_id
  path_part = "ingest"
}

# Define the method and integration req/res

resource "aws_api_gateway_method" "proxy" {
  rest_api_id = aws_api_gateway_rest_api.ingestion_api.id
  resource_id = aws_api_gateway_resource.root.id
  http_method = "POST"
  authorization = "NONE"
  api_key_required = true
}

resource "aws_api_gateway_integration" "lambda_integration" {
  rest_api_id = aws_api_gateway_rest_api.ingestion_api.id
  resource_id = aws_api_gateway_resource.root.id
  http_method = aws_api_gateway_method.proxy.http_method
  integration_http_method = "POST"
  type = "AWS"
  uri = aws_lambda_function.ingestion.invoke_arn
}

resource "aws_api_gateway_method_response" "proxy" {
  rest_api_id = aws_api_gateway_rest_api.ingestion_api.id
  resource_id = aws_api_gateway_resource.root.id
  http_method = aws_api_gateway_method.proxy.http_method
  status_code = "200"

  //cors
  response_parameters = {
    "method.response.header.Access-Control-Allow-Headers" = true,
    "method.response.header.Access-Control-Allow-Methods" = true,
    "method.response.header.Access-Control-Allow-Origin" = true
  }

}

resource "aws_api_gateway_integration_response" "proxy" {
  rest_api_id = aws_api_gateway_rest_api.ingestion_api.id
  resource_id = aws_api_gateway_resource.root.id
  http_method = aws_api_gateway_method.proxy.http_method
  status_code = aws_api_gateway_method_response.proxy.status_code


  //cors
  response_parameters = {
    "method.response.header.Access-Control-Allow-Headers" =  "'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token'",
    "method.response.header.Access-Control-Allow-Methods" = "'GET,OPTIONS,POST,PUT'",
    "method.response.header.Access-Control-Allow-Origin" = "'*'"
}

  depends_on = [
    aws_api_gateway_method.proxy,
    aws_api_gateway_integration.lambda_integration
  ]
}

# Deployment stage

resource "aws_api_gateway_deployment" "ingestion_deployment" {
  rest_api_id = aws_api_gateway_rest_api.ingestion_api.id
  
  triggers = {
    redeployment = sha1(jsonencode([
      aws_api_gateway_rest_api.ingestion_api.body,
      aws_api_gateway_method.proxy,
      aws_api_gateway_integration.lambda_integration
    ]))
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_api_gateway_stage" "ingestion_stage" {
  deployment_id = aws_api_gateway_deployment.ingestion_deployment.id
  rest_api_id   = aws_api_gateway_rest_api.ingestion_api.id
  stage_name    = "dap-${terraform.workspace}-ingestion-api-stage"
}

resource "aws_api_gateway_api_key" "ingestion_api_key" {
  name = "dap-${terraform.workspace}-ingestion-api-key"
}

resource "aws_api_gateway_usage_plan" "ingestion_usage_plan" {
  name = "dap-${terraform.workspace}-events-usage-plan"

  api_stages {
    api_id = aws_api_gateway_rest_api.ingestion_api.id
    stage  = aws_api_gateway_stage.ingestion_stage.stage_name
  }

  throttle_settings {
    burst_limit = 100
    rate_limit  = 50
  }
}

resource "aws_api_gateway_usage_plan_key" "ingestion_usage_plan_key" {
  key_id      = aws_api_gateway_api_key.ingestion_api_key.id
  key_type    = "API_KEY"
  usage_plan_id = aws_api_gateway_usage_plan.ingestion_usage_plan.id
}
