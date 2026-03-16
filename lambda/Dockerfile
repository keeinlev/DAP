FROM public.ecr.aws/lambda/python:3.12

COPY ingestion/requirements.txt .

RUN pip3 install --user -r requirements.txt

COPY ./ingestion ${LAMBDA_TASK_ROOT}
COPY ./generated/schemas ${LAMBDA_TASK_ROOT}/schemas

CMD ["handler.handler"]
