FROM public.ecr.aws/lambda/python:3.13

COPY requirements.txt ${LAMBDA_TASK_ROOT}

# Install python dependencies with optimizations
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir --resume-retries 3 -r requirements.txt

# Copy all files to root
COPY amend-extraction-2.py ${LAMBDA_TASK_ROOT}
COPY amd_main.py ${LAMBDA_TASK_ROOT}
COPY config.py ${LAMBDA_TASK_ROOT}
COPY amd_contract.py ${LAMBDA_TASK_ROOT}
COPY amd_salesforce_sub.py ${LAMBDA_TASK_ROOT}
COPY amd_docv.py ${LAMBDA_TASK_ROOT}
COPY amd_watchlist.py ${LAMBDA_TASK_ROOT}
COPY amd_fraud.py ${LAMBDA_TASK_ROOT}
COPY amd_workflow.py ${LAMBDA_TASK_ROOT}
COPY amd_electronic_id.py ${LAMBDA_TASK_ROOT}
COPY amd_kyb.py ${LAMBDA_TASK_ROOT}
COPY amd_discount_schedule.py ${LAMBDA_TASK_ROOT}
COPY amd_validation.py ${LAMBDA_TASK_ROOT}
COPY amd_std_check.py ${LAMBDA_TASK_ROOT}
COPY amd_logs.py ${LAMBDA_TASK_ROOT}
COPY amd_helper.py ${LAMBDA_TASK_ROOT}
COPY trulioo-413bdb6f7cd9.json ${LAMBDA_TASK_ROOT}

CMD ["amend-extraction-2.handler"]