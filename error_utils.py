import os
import json
import logging
from typing import Optional

import requests


def _get_webhook_url() -> Optional[str]:
    """Read Slack webhook URL from environment.

    Looks for `SLACK_WEBHOOK_URL` (preferred) or `SLACK_URL`.
    """
    return os.getenv("SLACK_WEBHOOK_URL") or os.getenv("SLACK_URL")


def send_error_to_slack(error_message: str, title: str = "Error") -> None:
    """Send an error message to Slack via Incoming Webhook.

    Expects the webhook URL in `.env` as `SLACK_WEBHOOK_URL` (or `SLACK_URL`).
    If the webhook is not configured or sending fails, logs a warning and returns.
    """
    webhook_url = _get_webhook_url()
    if not webhook_url:
        logging.debug("Slack webhook URL not configured; skipping Slack notification.")
        return

    payload = {"text": f"*{title}*\n{error_message}"}

    try:
        response = requests.post(
            webhook_url,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=5,
        )
        if response.status_code != 200 or response.text.strip() != "ok":
            logging.warning(
                "Failed to send message to Slack. status=%s response=%r",
                response.status_code,
                response.text,
            )
    except Exception as e:
        logging.warning("Exception while sending Slack notification: %s", e)


if __name__ == "__main__":
    send_error_to_slack("Something unexpected happened!", title="Server Warning")
