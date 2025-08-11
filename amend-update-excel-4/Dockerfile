FROM public.ecr.aws/lambda/python:3.13

COPY requirements.txt ${LAMBDA_TASK_ROOT}

# Install python dependencies
RUN pip install -r requirements.txt

# Copy all files to root
COPY amend-update-excel-4.py ${LAMBDA_TASK_ROOT}

CMD ["amend-update-excel-4.handler"]