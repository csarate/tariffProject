import json
import requests
import main_scraping
import boto3
import scrapers.utils_test.util as util


def handler(event, context, pais):
    bln_continuar = False
    str_message = ""
    if "ExecutionMode" not in event:
        event["ExecutionMode"] = "S3"

    if event["ExecutionMode"] not in ("S3", "Local"):
        raise ValueError("Invalid Execution Mode!!!")

    if event["ExecutionMode"] == "S3" and "S3Bucket" not in event:
        raise ValueError("S3Bucket was not specified!!!")

    if "ContinueLambdaName" not in event:
        event["ContinueLambdaName"] = "lambda-calden-scraper"

    # Ejecutar scraping si los parametros son correctos
    try:
        str_message, bln_continuar = main_scraping.ejecutar_scraping(event, pais)
    except Exception as e:
        raise e

    if bln_continuar:
        lambda_client = boto3.client('lambda')
        lambda_client.invoke(FunctionName=event["ContinueLambdaName"],
                             InvocationType='Event',
                             Payload=bytes(json.dumps(event), encoding='utf8'))
    return {
        'headers': {'Content-Type': 'application/json'},
        'statusCode': 200,
        'body': json.dumps({"message": str_message,
                            "raise_new_lambda": bln_continuar,
                            "event": event})
    }


