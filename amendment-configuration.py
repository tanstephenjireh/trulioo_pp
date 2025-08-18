{
  "Comment": "Amendment processor with parallel execution and retry logic",
  "StartAt": "ProcessAmendments",
  "States": {
    "ProcessAmendments": {
      "Type": "Map",
      "ItemsPath": "$.files",
      "MaxConcurrency": 5,
      "Parameters": {
        "bucket.$": "$.bucket",
        "key.$": "$$.Map.Item.Value.key",
        "fileName.$": "$$.Map.Item.Value.fileName",
        "size.$": "$$.Map.Item.Value.size",
        "extraction_id.$": "$.extraction_id",
        "user_id.$": "$.user_id"
      },
      "Iterator": {
        "StartAt": "ProcessSingleAmendment",
        "States": {
          "ProcessSingleAmendment": {
            "Type": "Task",
            "Resource": "arn:aws:lambda:ap-southeast-1:387082968990:function:amend-ocr-1",
            "Retry": [
              {
                "ErrorEquals": [
                  "States.TaskFailed"
                ],
                "IntervalSeconds": 5,
                "MaxAttempts": 5,
                "BackoffRate": 2
              }
            ],
            "Catch": [
              {
                "ErrorEquals": [
                  "States.ALL"
                ],
                "Next": "HandleAmendmentError"
              }
            ],
            "End": true
          },
          "HandleAmendmentError": {
            "Type": "Pass",
            "Result": {
              "status": "error",
              "stage": "amendment_processing_failed",
              "message": "Amendment OCR processing failed"
            },
            "End": true
          }
        }
      },
      "Next": "ProcessExtractedAmendments"
    },
    "ProcessExtractedAmendments": {
      "Type": "Map",
      "ItemsPath": "$",
      "MaxConcurrency": 5,
      "Parameters": {
        "status.$": "$$.Map.Item.Value.status",
        "message.$": "$$.Map.Item.Value.message",
        "fileName.$": "$$.Map.Item.Value.fileName",
        "bucket.$": "$$.Map.Item.Value.bucket",
        "key.$": "$$.Map.Item.Value.key",
        "size.$": "$$.Map.Item.Value.size",
        "extraction_id.$": "$$.Map.Item.Value.extraction_id",
        "user_id.$": "$$.Map.Item.Value.user_id",
        "contract_external_id.$": "$$.Map.Item.Value.contract_external_id",
        "customer_name.$": "$$.Map.Item.Value.customer_name",
        "parsedLocation.$": "$$.Map.Item.Value.parsedLocation",
        "processing_time.$": "$$.Map.Item.Value.processing_time"
      },
      "Iterator": {
        "StartAt": "ProcessSingleExtractedAmendment",
        "States": {
          "ProcessSingleExtractedAmendment": {
            "Type": "Task",
            "Resource": "arn:aws:lambda:ap-southeast-1:387082968990:function:amend-extraction-2",
            "Retry": [
              {
                "ErrorEquals": [
                  "States.TaskFailed"
                ],
                "IntervalSeconds": 5,
                "MaxAttempts": 5,
                "BackoffRate": 2
              }
            ],
            "Catch": [
              {
                "ErrorEquals": [
                  "States.ALL"
                ],
                "Next": "HandleSecondaryError"
              }
            ],
            "End": true
          },
          "HandleSecondaryError": {
            "Type": "Pass",
            "Result": {
              "status": "error",
              "stage": "secondary_processing_failed",
              "message": "Secondary amendment processing failed"
            },
            "End": true
          }
        }
      },
      "Next": "CollectResults"
    },
    "CollectResults": {
      "Type": "Pass",
      "Parameters": {
        "extraction_id.$": "$[0].extraction_id",
        "user_id.$": "$[0].user_id",
        "results.$": "$",
        "processing_complete": true,
        "timestamp.$": "$$.State.EnteredTime"
      },
      "Next": "ProcessResultsSequentially"
    },
    "ProcessResultsSequentially": {
      "Type": "Map",
      "ItemsPath": "$.results",
      "MaxConcurrency": 1,
      "Parameters": {
        "resultLocation.$": "$$.Map.Item.Value.resultLocation",
        "fileName.$": "$$.Map.Item.Value.fileName",
        "extraction_id.$": "$$.Map.Item.Value.extraction_id",
        "user_id.$": "$$.Map.Item.Value.user_id",
        "resultData.$": "$$.Map.Item.Value"
      },
      "Iterator": {
        "StartAt": "ProcessSingleResult",
        "States": {
          "ProcessSingleResult": {
            "Type": "Task",
            "Resource": "arn:aws:lambda:ap-southeast-1:387082968990:function:amend-sequential-update-3",
            "Retry": [
              {
                "ErrorEquals": [
                  "States.TaskFailed"
                ],
                "IntervalSeconds": 5,
                "MaxAttempts": 5,
                "BackoffRate": 2
              }
            ],
            "Catch": [
              {
                "ErrorEquals": [
                  "States.ALL"
                ],
                "Next": "HandleStep3Error"
              }
            ],
            "End": true
          },
          "HandleStep3Error": {
            "Type": "Pass",
            "Result": {
              "status": "error",
              "stage": "step3_processing_failed",
              "message": "Step 3 processing failed"
            },
            "End": true
          }
        }
      },
      "Next": "UpdateExcelFile"
    },
    "UpdateExcelFile": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:ap-southeast-1:387082968990:function:amend-update-excel-4",
      "Parameters": {
        "extraction_id.$": "$[0].extraction_id",
        "user_id.$": "$[0].user_id"
      },
      "Retry": [
        {
          "ErrorEquals": [
            "States.TaskFailed"
          ],
          "IntervalSeconds": 5,
          "MaxAttempts": 5,
          "BackoffRate": 2
        }
      ],
      "Catch": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "Next": "HandleExcelUpdateError"
        }
      ],
      "Next": "FinalResults"
    },
    "HandleExcelUpdateError": {
      "Type": "Pass",
      "Result": {
        "status": "error",
        "stage": "excel_update_failed",
        "message": "Excel file update failed"
      },
      "Next": "FinalResults"
    },
    "FinalResults": {
      "Type": "Pass",
      "Parameters": {
        "extraction_id.$": "$.extraction_id",
        "user_id.$": "$.user_id",
        "final_results.$": "$",
        "workflow_complete": true
      },
      "End": true
    }
  }
}