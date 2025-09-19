FROM public.ecr.aws/lambda/python:3.13

COPY requirements.txt ${LAMBDA_TASK_ROOT}

# Install python dependencies
RUN pip install -r requirements.txt

# Copy all files to root
COPY compare-accounts.py ${LAMBDA_TASK_ROOT}
COPY cmpr_og_sub.py ${LAMBDA_TASK_ROOT}
COPY cmpr_salesforce.py ${LAMBDA_TASK_ROOT}
COPY compr_main.py ${LAMBDA_TASK_ROOT}
COPY config.py ${LAMBDA_TASK_ROOT}

CMD ["compare-accounts.handler"]