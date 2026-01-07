from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import os
from dotenv import load_dotenv

load_dotenv()


def get_slack_client():
    slack_bot_token = os.getenv("SLACK_BOT_TOKEN")

    if not slack_bot_token:
        raise RuntimeError(
            f"""
            Environment variables not set:
                SLACK_BOT_TOKEN: {slack_bot_token}
            """
        )

    return WebClient(token=slack_bot_token)


def send_actual_trades_summary(filled_orders: list, channel: str = None):
    client = get_slack_client()
    channel = channel or os.getenv("SLACK_CHANNEL")

    if not channel:
        raise RuntimeError(
            "SLACK_CHANNEL environment variable not set and no channel provided"
        )

    if not filled_orders:
        message = {
            "channel": channel,
            "text": "✅ No trades executed today",
        }
        try:
            response = client.chat_postMessage(**message)
            return response
        except SlackApiError as e:
            raise RuntimeError(f"Error sending Slack message: {e.response['error']}")

    trade_lines = []
    for order in filled_orders:
        emoji = "📈" if order["side"] == "buy" else "📉"
        trade_lines.append(
            f"{emoji} {order['side'].upper()} {order['filled_qty']:.2f} shares of {order['ticker']} @ ${order['filled_avg_price']:.2f} = ${order['notional']:,.2f}"
        )

    trades_text = "\n".join(trade_lines)
    total_notional = sum(order["notional"] for order in filled_orders)

    message = {
        "channel": channel,
        "text": f"✅ Executed Trades Summary - {len(filled_orders)} trades filled",
        "blocks": [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": "✅ Executed Trades Report"},
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Total Trades Executed:* {len(filled_orders)}\n*Total Notional:* ${total_notional:,.2f}",
                },
            },
            {"type": "divider"},
            {"type": "section", "text": {"type": "mrkdwn", "text": trades_text}},
        ],
    }

    try:
        response = client.chat_postMessage(**message)
        return response
    except SlackApiError as e:
        raise RuntimeError(f"Error sending Slack message: {e.response['error']}")
