FROM public.ecr.aws/lambda/python:3.13

COPY requirements.txt ${LAMBDA_TASK_ROOT}

# Install python dependencies
RUN pip install -r requirements.txt

# Copy all files to root
COPY 2_extract_data.py ${LAMBDA_TASK_ROOT}
COPY config.py ${LAMBDA_TASK_ROOT}
COPY contract_extractor.py ${LAMBDA_TASK_ROOT}
COPY docv.py ${LAMBDA_TASK_ROOT}
COPY salesforce.py ${LAMBDA_TASK_ROOT}
COPY watchlist.py ${LAMBDA_TASK_ROOT}
COPY fraud.py ${LAMBDA_TASK_ROOT}
COPY workflow.py ${LAMBDA_TASK_ROOT}
COPY validation.py ${LAMBDA_TASK_ROOT}
COPY trulioo-af66af3a3788.json ${LAMBDA_TASK_ROOT}

CMD ["2_extract_data.handler"]