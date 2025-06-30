FROM public.ecr.aws/lambda/python:3.13

COPY requirements.txt ${LAMBDA_TASK_ROOT}

# Install python dependencies
RUN pip install -r requirements.txt

# Copy all files to root
COPY lambda_function.py ${LAMBDA_TASK_ROOT}
COPY config.py ${LAMBDA_TASK_ROOT}
COPY pdf_parser.py ${LAMBDA_TASK_ROOT}
COPY contract_extractor.py ${LAMBDA_TASK_ROOT}
COPY salesforce.py ${LAMBDA_TASK_ROOT}
COPY docv.py ${LAMBDA_TASK_ROOT}
COPY watchlist.py ${LAMBDA_TASK_ROOT}

CMD ["lambda_function.handler"]