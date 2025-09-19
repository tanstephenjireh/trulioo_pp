FROM public.ecr.aws/lambda/python:3.13

COPY requirements.txt ${LAMBDA_TASK_ROOT}

# Install python dependencies
RUN pip install -r requirements.txt

# Copy all files to root
COPY delete-lis-2.py ${LAMBDA_TASK_ROOT}
COPY cmpr_delete_lis.py ${LAMBDA_TASK_ROOT}

CMD ["delete-lis-2.handler"]