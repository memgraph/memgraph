import json
import os


def trigger_mage_payload(date: int) -> dict:
    """
    build the dict to be dumped as JSON and used to trigger the MAGE daily build
    """

    payload = {
        "event_type": "trigger_daily_build",
        "client_payload": {
            "date": date
        }
    }

    return payload


def main() -> None:
    """
    Build request payload and print JSON to be used by curl to trigger the MAGE
    daily build process.

    This function should probably be extended to pass on test results etc.
    """

    date = int(os.getenv("CURRENT_BUILD_DATE"))

    payload = trigger_mage_payload(date)
    print(json.dumps(payload))


if __name__ == "__main__":
    main()
