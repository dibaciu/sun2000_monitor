import json
import os
import random
import string

INFLUXDB3_ADMIN_TOKEN_FILE = 'influxdb3.admin.token'
INFLUXDB_EXPLORER_CONFIG_FILE = 'influxdb3-explorer.config'
COMMON_ENV_TEMPLATE_FILE = '.env.template'
COMMON_ENV_FILE = '.env'
INFLUXDB_EXPLORER_ENV_FILE = '.env.explorer'
INFLUXDB_EXPLORER_ENV_TEMPLATE_FILE = '.env.explorer.template'
GRAFANA_ENV_FILE = '.env.grafana'
GRAFANA_ENV_TEMPLATE_FILE = '.env.grafana.template'
MONITOR_ENV_FILE = '.env.monitor'
MONITOR_ENV_TEMPLATE_FILE = '.env.monitor.template'
DOCKER_COMPOSE_FILE = 'docker-compose.yaml'
DOCKER_COMPOSE_TEMPLATE_FILE = 'docker-compose.yaml.template'

def generate_influxdb_token(length=86):
    chars = string.ascii_letters + string.digits + '-'
    return f'api3_{"".join(random.choices(chars, k=length))}'

def generate_explorer_session_secret_key(length=64):
    chars = string.ascii_letters + string.digits + '-'
    return "".join(random.choices(chars, k=length))


for config_files in [COMMON_ENV_FILE, INFLUXDB_EXPLORER_ENV_FILE, GRAFANA_ENV_FILE, MONITOR_ENV_FILE, INFLUXDB3_ADMIN_TOKEN_FILE, INFLUXDB_EXPLORER_CONFIG_FILE, DOCKER_COMPOSE_FILE]:
    assert not os.path.exists(config_files), f'Config file {config_files} already exists. Please remove it before running this script.'

os.environ["INFLUXDB_HOST"] = input("InfluxDB Host (default influx): ") or "influx"
os.environ["INFLUXDB_PORT"] = input("InfluxDB Port (default 8181): ") or "8181"
os.environ["INFLUXDB_DBNAME"] = "sun2000_monitoring"
os.environ["INFLUXDB_ADMIN_TOKEN"] = generate_influxdb_token()
os.environ["EXPLORER_SESSION_SECRET_KEY"] = generate_explorer_session_secret_key()
os.environ["EXPLORER_PORT"] = input("InfluxDB Explorer Port (default 8888): ") or "8888"
os.environ["GRAFANA_ADMIN_USER"] = input("Grafana Admin User (default admin): ") or "admin"
os.environ["GRAFANA_PORT"] = input("Grafana Port (default 3000): ") or "3000"
os.environ["GRAFANA_ADMIN_PASSWORD"] = input("Grafana Admin Password (default admin): ") or "admin"
os.environ["SUN2000_INVERTER_HOST"] = input("Sun2000 Inverter Host/IP: ")
os.environ["SUN2000_INVERTER_PORT"] = input("Sun2000 Inverter Port (default 6607): ") or "6607"
os.environ["POLLING_INTERVAL_SECONDS"] = input("Polling Interval Seconds (default 5): ") or "5"

with open(COMMON_ENV_TEMPLATE_FILE) as f:
    template_common_env = string.Template(f.read())
with open(COMMON_ENV_FILE, 'w') as f:
    f.write(template_common_env.substitute(os.environ))

with open(INFLUXDB_EXPLORER_ENV_TEMPLATE_FILE) as f:
    template_explorer_env = string.Template(f.read())
with open(INFLUXDB_EXPLORER_ENV_FILE, 'w') as f:
    f.write(template_explorer_env.substitute(os.environ))

with open(GRAFANA_ENV_TEMPLATE_FILE) as f:
    template_grafana_env = string.Template(f.read())
with open(GRAFANA_ENV_FILE, 'w') as f:
    f.write(template_grafana_env.substitute(os.environ))

with open(MONITOR_ENV_TEMPLATE_FILE) as f:
    template_monitor_env = string.Template(f.read())
with open(MONITOR_ENV_FILE, 'w') as f:
    f.write(template_monitor_env.substitute(os.environ))

influxdb_admin_token_dict = {
    'name': 'admin',
    'token': os.environ['INFLUXDB_ADMIN_TOKEN']
}
with open(INFLUXDB3_ADMIN_TOKEN_FILE, 'w') as f:
    f.write(json.dumps(influxdb_admin_token_dict))

influxdb_explorer_config_dict = {
    'DEFAULT_INFLUX_SERVER': f'http://{os.environ["INFLUXDB_HOST"]}:{os.environ["INFLUXDB_PORT"]}',
    'DEFAULT_INFLUX_DATABASE': os.environ['INFLUXDB_DBNAME'],
    'DEFAULT_API_TOKEN': os.environ['INFLUXDB_ADMIN_TOKEN'],
    'DEFAULT_SERVER_NAME': 'InfluxDB 3'
}
with open(INFLUXDB_EXPLORER_CONFIG_FILE, 'w') as f:
    f.write(json.dumps(influxdb_explorer_config_dict))

with open(DOCKER_COMPOSE_TEMPLATE_FILE) as f:
    template_docker_compose = string.Template(f.read())
with open(DOCKER_COMPOSE_FILE, 'w') as f:
    f.write(template_docker_compose.substitute(os.environ))

print('Generated environment files:')
print(f' - {COMMON_ENV_FILE}')
print(f' - {INFLUXDB_EXPLORER_ENV_FILE}')
print(f' - {GRAFANA_ENV_FILE}')
print(f' - {MONITOR_ENV_FILE}')
print(f' - {INFLUXDB3_ADMIN_TOKEN_FILE}')
print(f' - {INFLUXDB_EXPLORER_CONFIG_FILE}')
print(f' - {DOCKER_COMPOSE_FILE}')

print('Setup complete. You can now build and start the containers with:')
print(f'  docker-compose -f {DOCKER_COMPOSE_FILE} build')
print(f'  docker-compose -f {DOCKER_COMPOSE_FILE} up -d')
