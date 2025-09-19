FROM public.ecr.aws/lambda/python:3.13

COPY requirements.txt ${LAMBDA_TASK_ROOT}

# Install python dependencies
RUN pip install -r requirements.txt

# Copy all files to root
COPY add-sub-1.py ${LAMBDA_TASK_ROOT}
COPY cmpr_sub_add.py ${LAMBDA_TASK_ROOT}
COPY config.py ${LAMBDA_TASK_ROOT}

CMD ["add-sub-1.handler"]