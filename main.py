import threading
import time
import requests
from faker import Faker

# === GENERAL Config ===
DURATION_SECONDS = 600
ACCESS_TOKEN = ''

# === GET Config ===
WAIT_TIME_MS_GET = 200
ENDPOINT_GET = "http://localhost:9090/api/personen/43dc90ba-4d6a-4e7b-8cea-fc61c96fdd43"
HEADERS_GET = {
    'Authorization': 'Bearer ' + ACCESS_TOKEN
}

# === POST Config ===
WAIT_TIME_MS_POST = 400
ENDPOINT_POST = "http://localhost:9090/api/personenkontext-workflow"
HEADERS_POST = {
    'Content-Type': 'application/json; charset=utf-8',
    'Authorization': 'Bearer ' + ACCESS_TOKEN
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
                    "organisationId": "17aa46ec-eb5f-4efc-b1f5-dafa27d5bf70",
                    "rolleId": "17c6cc62-4405-4d32-9efd-0bd5e79a2b74"
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
