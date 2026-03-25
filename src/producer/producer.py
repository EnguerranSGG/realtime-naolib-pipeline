import json
import os
import time
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer


def load_config() -> dict:
    load_dotenv()

    config = {
        "api_url": os.getenv("API_URL"),
        "fetch_interval": int(os.getenv("FETCH_INTERVAL", "30")),
        "kafka_broker": os.getenv("KAFKA_BROKER", "localhost:29092"),
        "kafka_topic": os.getenv("KAFKA_TOPIC", "naolib-realtime"),
    }

    missing = [key for key, value in config.items() if value in (None, "")]
    if missing:
        raise ValueError(
            f"Variables d'environnement manquantes : {', '.join(missing)}"
        )

    return config


def create_kafka_producer(bootstrap_server: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_server,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8") if key else None,
    )


def fetch_api_data(api_url: str) -> dict:
    response = requests.get(api_url, timeout=15)
    response.raise_for_status()
    return response.json()


def normalize_payload(api_response: dict) -> list[dict]:
    """
    Essaie de gérer plusieurs formes de payload :
    - {"results": [...]}
    - {"records": [...]}
    - autre -> encapsulé tel quel dans une liste
    """
    if isinstance(api_response, dict):
        if "results" in api_response and isinstance(api_response["results"], list):
            return api_response["results"]

        if "records" in api_response and isinstance(api_response["records"], list):
            return api_response["records"]

    return [api_response]


def build_message(record: dict) -> dict:
    return {
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "source": "naolib-api",
        "data": record,
    }


def extract_message_key(record: dict) -> str | None:
    """
    On essaie de trouver une clé métier stable.
    Selon la structure réelle de l'API, tu pourras l'ajuster.
    """
    for candidate in ("stop_id", "id", "code", "stationcode", "stop_code"):
        value = record.get(candidate)
        if value is not None:
            return str(value)

    return None


def main() -> None:
    config = load_config()
    producer = create_kafka_producer(config["kafka_broker"])

    print("Producer démarré.")
    print(f"API_URL        = {config['api_url']}")
    print(f"KAFKA_BROKER   = {config['kafka_broker']}")
    print(f"KAFKA_TOPIC    = {config['kafka_topic']}")
    print(f"FETCH_INTERVAL = {config['fetch_interval']}s")

    while True:
        try:
            payload = fetch_api_data(config["api_url"])
            records = normalize_payload(payload)

            print(f"{len(records)} enregistrement(s) récupéré(s) depuis l'API.")

            for record in records:
                message = build_message(record)
                key = extract_message_key(record)

                future = producer.send(
                    topic=config["kafka_topic"],
                    key=key,
                    value=message,
                )
                metadata = future.get(timeout=10)

                print(
                    f"Message envoyé -> topic={metadata.topic}, "
                    f"partition={metadata.partition}, offset={metadata.offset}, key={key}"
                )

            producer.flush()

        except requests.RequestException as exc:
            print(f"Erreur HTTP/API : {exc}")

        except Exception as exc:
            print(f"Erreur inattendue : {exc}")

        time.sleep(config["fetch_interval"])


if __name__ == "__main__":
    main()