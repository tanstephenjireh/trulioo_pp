FROM public.ecr.aws/lambda/python:3.13

COPY requirements.txt ${LAMBDA_TASK_ROOT}

# Install python dependencies
RUN pip install -r requirements.txt

# Copy all files to root
COPY amend-sequential-3.py ${LAMBDA_TASK_ROOT}
COPY amd_overwrite.py ${LAMBDA_TASK_ROOT}

CMD ["amend-sequential-3.handler"]