"""Interactive setup using only Python's standard library. Never prints the bot token."""
from pathlib import Path
import getpass
import json
import os
import secrets
import subprocess
import sys
import urllib.request


def main():
    root = Path(__file__).resolve().parent
    env_file = root / '.env'
    if env_file.exists():
        print('.env already exists; preserving settings. To start: docker compose up -d --build')
        return
    token = getpass.getpass('Paste the token from @BotFather (hidden): ').strip()
    if ':' not in token or any(c.isspace() for c in token):
        raise SystemExit('Invalid token format.')
    try:
        with urllib.request.urlopen('https://api.telegram.org/bot' + token + '/getMe', timeout=20) as response:
            data = json.load(response)
        if not data.get('ok'): raise ValueError()
    except Exception:
        raise SystemExit('Cannot verify Telegram token. Check token, internet and VPN; nothing saved.')
    code = secrets.token_urlsafe(18)
    fd = os.open(env_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'w') as file:
        file.write(f'TELEGRAM_BOT_TOKEN={token}\nPAIRING_CODE={code}\nMAX_SYMBOLS_PER_EXCHANGE=40\n')
    print('Configured @' + data['result']['username'])
    print('After starting, send this command to YOUR bot in a private chat:')
    print('/start ' + code)
    print('Start: docker compose up -d --build')
    print('Status: docker compose logs --tail=80 -f bot')

if __name__ == '__main__': main()
