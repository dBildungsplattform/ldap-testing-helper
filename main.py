import threading
import time
import os
import requests
from faker import Faker

# === GENERAL Config ===
DURATION_SECONDS = 10000000
ACCESS_COOKIE = os.environ.get('ACCESS_COOKIE')

# === GET Config ===
WAIT_TIME_MS_GET = 3500
ENDPOINT_GET = "https://spsh.staging.spsh.dbildungsplattform.de/api/personen/1e8decc9-a2a7-415a-9470-f1dc81ef5b53"
HEADERS_GET = {
    'Cookie': ACCESS_COOKIE
}

# === POST Config ===
WAIT_TIME_MS_POST = 5000
ENDPOINT_POST = "https://spsh.staging.spsh.dbildungsplattform.de/api/personenkontext-workflow"
HEADERS_POST = {
    'Content-Type': 'application/json; charset=utf-8',
    'Cookie': ACCESS_COOKIE
}

faker = Faker("de_DE")

# === Helper Logger ===
def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}")

# === Request Functions ===
def call_endpoint_get():
    end_time = time.time() + DURATION_SECONDS
    while time.time() < end_time:
        try:
            response = requests.get(ENDPOINT_GET, timeout=10, headers=HEADERS_GET)
            if response.ok:
                log(f"GET A - Status: {response.status_code}")
            else:
                log(f"GET A - Failed Status: {response.status_code}, Body: {response.text}")
        except Exception:
            log("GET A - Request failed or timed out")
        time.sleep(WAIT_TIME_MS_GET / 1000)

def call_endpoint_post():
    end_time = time.time() + DURATION_SECONDS
    while time.time() < end_time:
        try:
            body = {
                "familienname": faker.last_name(),
                "vorname": faker.first_name(),
                "personalnummer": faker.unique.random_number(digits=6, fix_len=True),
                "createPersonenkontexte": [
                    {
                    "organisationId": "2f5c11f5-f319-4365-b93c-bf074dab096c",
                    "rolleId": "4b4ecff2-2967-4318-8f38-94b337c79d46"
                    }
                ]
            }
            response = requests.post(ENDPOINT_POST, json=body, timeout=10, headers=HEADERS_POST)
            if response.ok:
                log(f"POST B - Status: {response.status_code}")
            else:
                log(f"POST B - Failed Status: {response.status_code}, Body: {response.text}")
        except Exception:
            log("POST B - Request failed or timed out")
        time.sleep(WAIT_TIME_MS_POST / 1000)

# === Main ===
def main():
    log("")
    log("##########################")
    log("# Main execution started #")
    log("##########################")

    thread_a = threading.Thread(target=call_endpoint_get)
    thread_b = threading.Thread(target=call_endpoint_post)

    thread_a.start()
    thread_b.start()

    thread_a.join()
    thread_b.join()

    log("")
    log("###########################")
    log("# Main execution finished #")
    log("###########################")

if __name__ == "__main__":
    main()
