import json
import main_scraping
import boto3
import datetime

def handler(event, context):
    bln_continuar: bool = False
    str_message: str = ""
    if "ExecutionMode" not in event:
        event["ExecutionMode"] = "S3"

    if event["ExecutionMode"] not in ("S3", "Local"):
        raise ValueError("Invalid Execution Mode!!!")

    if event["ExecutionMode"] == "S3" and "S3Bucket" not in event:
        raise ValueError("S3Bucket was not specified!!!")

    if "ContinueLambdaName" not in event:
        event["ContinueLambdaName"] = "lambda-calden-scraper"

    # Max execution time in seconds
    if "ExecutionTimeMax" not in event:
        event["ExecutionTimeMax"] = 600
    else:
        event["ExecutionTimeMax"] = int(event["ExecutionTimeMax"])

    # Countries List
    if "Countries" not in event:
        event["Countries"] = ["argentina", "el_salvador", "nicaragua", "mexico", "panama", "españa"]

    event["StartDateTime"] = datetime.datetime.now()

    # Ejecutar scraping si los parametros son correctos
    try:
        str_message, bln_continuar = main_scraping.ejecutar_scraping(event)
    except Exception as e:
        raise e

    del event["StartDateTime"]
    event["ExecutionTimeMax"] = str(event["ExecutionTimeMax"])

    if bln_continuar and event["ExecutionMode"] == "S3":
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