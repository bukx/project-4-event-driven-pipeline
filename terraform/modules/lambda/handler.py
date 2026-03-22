"""Lambda function triggered by EventBridge for lightweight event routing."""
import json, os, logging, boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
    """Route events based on type to different processing targets."""
    logger.info(f"Received event: {json.dumps(event)}")

    event_type = event.get("detail", {}).get("event_type", "unknown")
    source = event.get("source", "unknown")

    if event_type == "purchase":
        # Forward high-value events to SNS for notifications
        sns = boto3.client("sns")
        sns.publish(
            TopicArn=os.environ["NOTIFICATION_TOPIC_ARN"],
            Subject=f"New purchase from {source}",
            Message=json.dumps(event["detail"]),
        )
        logger.info(f"Purchase event forwarded to SNS")

    elif event_type == "sensor_reading":
        temp = event.get("detail", {}).get("temperature_c", 0)
        if temp > 40:
            logger.warning(f"High temperature alert: {temp}C from {event['detail'].get('sensor_id')}")

    return {"statusCode": 200, "processed": event_type}
