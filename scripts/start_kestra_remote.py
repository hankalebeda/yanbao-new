"""SSH to Ubuntu and start Kestra Docker stack."""
import paramiko
import time
import sys

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect("192.168.232.141", username="hugh", password="Qwer1234", timeout=10)

commands = [
    "cd /home/hugh/yanbao/automation/deploy && docker compose -f docker-compose.kestra.yml ps 2>&1",
    "cd /home/hugh/yanbao/automation/deploy && docker compose -f docker-compose.kestra.yml up -d 2>&1",
]

for cmd in commands:
    print(f">>> {cmd[:80]}")
    stdin, stdout, stderr = ssh.exec_command(cmd, timeout=60)
    out = stdout.read().decode()
    err = stderr.read().decode()
    if out:
        print(out)
    if err:
        print("STDERR:", err)
    print("---")

print("Waiting 10s for Kestra to start...")
time.sleep(10)

stdin, stdout, stderr = ssh.exec_command(
    "curl -s -o /dev/null -w '%{http_code}' http://localhost:18080/api/v1/flows", timeout=15
)
code = stdout.read().decode().strip()
print(f"Kestra HTTP status: {code}")

ssh.close()
sys.exit(0 if code in ("200", "401") else 1)
