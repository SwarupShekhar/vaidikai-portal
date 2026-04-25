import json
import secrets
import sys
from datetime import date
from pathlib import Path

CLIENTS_FILE = Path(__file__).parent / "clients.json"


def load_clients():
    if not CLIENTS_FILE.exists():
        return {}
    with open(CLIENTS_FILE) as f:
        return json.load(f)


def save_clients(clients):
    with open(CLIENTS_FILE, "w") as f:
        json.dump(clients, f, indent=2)


def next_client_code(clients):
    existing = {v["client_code"] for v in clients.values()}
    n = 1
    while True:
        code = f"CLIENT{n:03d}"
        if code not in existing:
            return code
        n += 1


def add_client(name, email=""):
    clients = load_clients()
    token = secrets.token_urlsafe(16)
    client_code = next_client_code(clients)
    clients[token] = {
        "client_code": client_code,
        "client_name": name,
        "active": True,
        "created_at": str(date.today()),
        "contact_email": email,
    }
    save_clients(clients)
    print(f"Client added:  {name}")
    print(f"Client code:   {client_code}")
    print(f"Access URL:    http://localhost:8000/access/{token}")
    return token, client_code


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print('Usage: python add_client.py "Client Name" "contact@email.com"')
        sys.exit(1)
    client_name = sys.argv[1]
    contact_email = sys.argv[2] if len(sys.argv) > 2 else ""
    add_client(client_name, contact_email)
