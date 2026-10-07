import urllib.request
import json
import sys

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

try:
    req = urllib.request.Request('http://localhost:20335/api/stats')
    with urllib.request.urlopen(req) as resp:
        d = json.loads(resp.read().decode())
        print('Total accounts in API:', len(d.get('accounts', [])))
        for a in d.get('accounts', []):
            print(f"UID: {a.get('uid')} | Region: {a.get('region')} | Status: {a.get('status')} | Level: {a.get('level')} | Exp: {a.get('current_exp')} (+{a.get('gained_exp')}) | Matches: {a.get('matches_played')} / {a.get('matches_started')}")
        print("\n--- LATEST 25 LOGS ---")
        for l in d.get('logs', [])[-25:]:
            print(f"[{l.get('type')}] {l.get('message')}")
except Exception as e:
    print("Error:", e)
