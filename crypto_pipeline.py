from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.models import Variable
from datetime import datetime
import requests
import json
import os

PRICE_HISTORY_FILE = "/opt/airflow/dags/price_history.json"

def extract_prices():
    coins = Variable.get("crypto_coins")
    coin_ids = ",".join(coins.split(","))
    url = f"https://api.coingecko.com/api/v3/simple/price?ids={coin_ids}&vs_currencies=usd"
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    data = response.json()
    print(f"Fetched prices: {data}")
    return data


def check_price_changes(ti):
    current_prices = ti.xcom_pull(task_ids="extract_prices")
    threshold = float(Variable.get("alert_threshold_percent"))

    if os.path.exists(PRICE_HISTORY_FILE):
        with open(PRICE_HISTORY_FILE, "r") as f:
            previous_prices = json.load(f)
    else:
        previous_prices = {}

    alerts = []
    for coin, price_data in current_prices.items():
        current = price_data["usd"]
        if coin in previous_prices:
            previous = previous_prices[coin]["usd"]
            change_percent = abs((current - previous) / previous) * 100
            if change_percent >= threshold:
                alerts.append(f"{coin}: {previous} → {current} ({change_percent:.1f}% change)")

    with open(PRICE_HISTORY_FILE, "w") as f:
        json.dump(current_prices, f)

    print(f"Alerts found: {alerts}")
    return alerts  


def decide_branch(ti):
    """Decides which path the DAG takes next, based on whether alerts exist."""
    alerts = ti.xcom_pull(task_ids="check_price_changes")
    if alerts:
        return "send_alert_email"
    return "skip_email"


with DAG(
    dag_id="crypto_market_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    check_api = HttpSensor(
        task_id="check_api_available",
        http_conn_id="coingecko_api",
        endpoint="/api/v3/ping",
        timeout=20,
        poke_interval=5,
    )

    extract_task = PythonOperator(
        task_id="extract_prices",
        python_callable=extract_prices,
    )

    check_task = PythonOperator(
        task_id="check_price_changes",
        python_callable=check_price_changes,
    )

    branch_task = BranchPythonOperator(
        task_id="decide_branch",
        python_callable=decide_branch,
    )

    send_alert_email = EmailOperator(
        task_id="send_alert_email",
        to="islamhassan2510@gmail.com",
        subject=" Crypto Price Alert",
        html_content="Significant price changes detected. Check Airflow logs for details.",
    )

    skip_email = EmptyOperator(task_id="skip_email")

    check_api >> extract_task >> check_task >> branch_task
    branch_task >> send_alert_email
    branch_task >> skip_email