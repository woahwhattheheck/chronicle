"""Bounded real-Mosquitto delivery probe for Chronicle's home automation example.
Run after `go build -o home-automation .` and `docker compose up -d mosquitto`.
No physical-device or Raspberry Pi benchmark is implied by this host fixture.
"""
from __future__ import annotations
import concurrent.futures
import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

ROOT = Path.cwd()
OUT = Path(os.environ.get('DELIVERY_OUT', ROOT / 'delivery-results')).resolve()
OUT.mkdir(parents=True, exist_ok=True)
PORT = 18086
BASE = f'http://127.0.0.1:{PORT}'
REPORT = {'source_sha': os.environ.get('SOURCE_SHA', ''), 'host': 'GitHub-hosted Linux x86_64; real Mosquitto, no physical sensor/Pi hardware', 'checks': [], 'peak_rss_kib': 0}
PROC = None
LOG = None
WORK = tempfile.TemporaryDirectory(prefix='chronicle-mqtt-delivery-')


def check(name, data=None):
    REPORT['checks'].append({'name': name, 'result': 'PASS', 'detail': data})
    print('PASS', name, json.dumps(data), flush=True)


def http(path, body=None, header=True):
    headers = {'Content-Type': 'application/json'}
    if header:
        headers['X-Requested-With'] = 'XMLHttpRequest'
    request = urllib.request.Request(BASE + path, data=None if body is None else json.dumps(body).encode(), headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return response.status, response.read().decode()
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode()


def rss():
    if PROC is None or PROC.poll() is not None:
        return
    try:
        for line in Path(f'/proc/{PROC.pid}/status').read_text().splitlines():
            if line.startswith(('VmHWM:', 'VmRSS:')):
                REPORT['peak_rss_kib'] = max(REPORT['peak_rss_kib'], int(line.split()[1]))
    except OSError:
        pass


def until(predicate, label, timeout=45):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        if PROC is not None and PROC.poll() is not None:
            raise AssertionError(f'collector exited early with {PROC.returncode}: {label}')
        rss()
        try:
            value = predicate()
            if value:
                return value
        except (OSError, ValueError, urllib.error.URLError) as error:
            last = str(error)
        time.sleep(0.25)
    raise AssertionError(f'timeout: {label}; last error={last}')


def start(number):
    global PROC, LOG
    LOG = (OUT / f'collector-{number}.log').open('w')
    PROC = subprocess.Popen([str(ROOT / 'home-automation'), '-broker', 'tcp://127.0.0.1:1883', '-db', str(Path(WORK.name) / 'home.db'), '-rooms', 'rooms.example.json', '-http-port', str(PORT)], stdout=LOG, stderr=subprocess.STDOUT)
    until(lambda: http('/health')[0] == 200, 'HTTP health')
    until(lambda: 'MQTT subscribed:' in (OUT / f'collector-{number}.log').read_text(), 'MQTT subscription')
    check(f'collector_{number}_started')


def stop(number):
    global PROC, LOG
    rss()
    PROC.send_signal(signal.SIGTERM)
    try:
        code = PROC.wait(timeout=25)
    except subprocess.TimeoutExpired:
        PROC.kill()
        PROC.wait(timeout=5)
        raise AssertionError('graceful shutdown exceeded 25 seconds')
    LOG.close()
    PROC = None
    text = (OUT / f'collector-{number}.log').read_text()
    print(text[-12000:], flush=True)
    assert code == 0, f'collector shutdown returned {code}'
    check(f'collector_{number}_graceful_shutdown', {'exit_code': code})


def publish(topic, payload):
    text = payload if isinstance(payload, str) else json.dumps(payload)
    subprocess.run(['docker', 'compose', 'exec', '-T', 'mosquitto', 'mosquitto_pub', '-h', 'localhost', '-q', '1', '-t', topic, '-m', text], check=True, timeout=15)


def points(node):
    if isinstance(node, dict):
        lower = {str(k).lower(): v for k, v in node.items()}
        if isinstance(lower.get('tags'), dict) and 'value' in lower:
            yield lower
        else:
            for value in node.values():
                yield from points(value)
    elif isinstance(node, list):
        for value in node:
            yield from points(value)


def query(metric):
    start_time = time.perf_counter()
    code, body = http('/query', {'metric': metric})
    assert code == 200, f'query returned HTTP {code}: {body[:1000]}'
    data = json.loads(body)
    REPORT.setdefault('query_latency_ms', []).append(round((time.perf_counter() - start_time) * 1000, 3))
    return list(points(data))


def has(metric, device, value):
    return any(p['tags'].get('device') == device and p['value'] == value for p in query(metric))


def main():
    start(1)
    code, body = http('/query', {'metric': 'temperature_c'}, header=False)
    REPORT['documented_request'] = {'http_status': code, 'body': body[:1500]}
    print('DOCUMENTED_QUERY', code, body[:1500], flush=True)
    code, body = http('/query', {'metric': 'temperature_c'}, header=True)
    print('HEADER_QUERY', code, body[:1500], flush=True)
    assert code == 200, f'query with middleware header failed: {code}: {body}'
    check('query_endpoint_with_required_header')
    publish('zigbee2mqtt/living-room-sensor', {'temperature': 22.5, 'humidity': 45, 'occupancy': True})
    publish('home/zwave/front-door/door-1/contact_open', {'value': True})
    publish('home/mqtt/office/esp-1/temperature_c', '21.75')
    publish('zigbee2mqtt/kitchen-plug', {'power': 65.1, 'energy': 1.234})
    expected = [('temperature_c', 'living-room-sensor', 22.5), ('humidity_pct', 'living-room-sensor', 45), ('motion', 'living-room-sensor', 1), ('contact_open', 'door-1', 1), ('temperature_c', 'esp-1', 21.75), ('power_w', 'kitchen-plug', 65.1), ('energy_kwh', 'kitchen-plug', 1.234)]
    for metric, device, value in expected:
        until(lambda m=metric, d=device, v=value: has(m, d, v), f'{metric}/{device} stored')
    check('real_mqtt_four_devices_seven_points_six_metrics', expected)
    living = [p for p in query('temperature_c') if p['tags'].get('device') == 'living-room-sensor']
    assert living and living[0]['tags'].get('room') == 'living-room'
    check('room_map_preserved')
    before = len(query('temperature_c'))
    publish('zigbee2mqtt/living-room-sensor', '{bad json')
    publish('zigbee2mqtt/living-room-sensor', {'temperature': 'invalid'})
    publish('zigbee2mqtt/bridge/state', {'state': 'online'})
    publish('zigbee2mqtt/living-room-sensor/availability', {'state': 'offline'})
    publish('home/mqtt/office/esp-1/temperature_c', '23.25')
    until(lambda: has('temperature_c', 'esp-1', 23.25), 'valid report after malformed/offline reports')
    assert len(query('temperature_c')) == before + 1
    check('malformed_control_and_offline_reports_do_not_invent_readings')
    def concurrent_read():
        for _ in range(12):
            assert query('temperature_c')
            time.sleep(0.02)
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        reads = pool.submit(concurrent_read)
        for value in range(24, 29):
            publish('home/mqtt/office/esp-1/temperature_c', str(value))
        reads.result(timeout=30)
    until(lambda: has('temperature_c', 'esp-1', 28), 'concurrent writes completed')
    check('queries_continue_during_ingestion')
    old_subscriptions = (OUT / 'collector-1.log').read_text().count('MQTT subscribed:')
    subprocess.run(['docker', 'compose', 'stop', 'mosquitto'], check=True, timeout=30)
    assert query('temperature_c')
    check('query_api_available_while_broker_offline')
    subprocess.run(['docker', 'compose', 'start', 'mosquitto'], check=True, timeout=30)
    until(lambda: (OUT / 'collector-1.log').read_text().count('MQTT subscribed:') > old_subscriptions, 'resubscribe after broker restart', timeout=75)
    publish('home/mqtt/office/esp-1/temperature_c', '29.5')
    until(lambda: has('temperature_c', 'esp-1', 29.5), 'post-reconnect reading')
    check('broker_restart_resubscription_and_ingestion')
    saved = query('temperature_c')
    stop(1)
    start(2)
    reopened = query('temperature_c')
    assert sorted(json.dumps(p, sort_keys=True) for p in saved) == sorted(json.dumps(p, sort_keys=True) for p in reopened)
    check('graceful_restart_preserves_exact_temperature_history', {'points': len(saved)})
    for metric, device, value in expected:
        assert has(metric, device, value), f'restart lost {metric}/{device}'
    check('all_six_metric_histories_survive_restart')
    publish('home/mqtt/office/esp-1/temperature_c', '30.75')
    until(lambda: has('temperature_c', 'esp-1', 30.75), 'post-restart new write')
    check('ingestion_resumes_after_database_reopen')
    stop(2)
    REPORT['status'] = 'PASS'


if __name__ == '__main__':
    started = time.monotonic()
    try:
        main()
    except BaseException as error:
        REPORT['status'] = 'FAIL'
        REPORT['error'] = repr(error)
        raise
    finally:
        if PROC is not None and PROC.poll() is None:
            PROC.kill()
            PROC.wait(timeout=5)
        if LOG is not None and not LOG.closed:
            LOG.close()
        REPORT['elapsed_seconds'] = round(time.monotonic() - started, 3)
        latencies = REPORT.pop('query_latency_ms', [])
        if latencies:
            ordered = sorted(latencies)
            REPORT['query_latency_ms'] = {'count': len(ordered), 'p50': ordered[len(ordered)//2], 'p95': ordered[min(len(ordered)-1, int(len(ordered)*.95))], 'max': max(ordered)}
        (OUT / 'receipt.json').write_text(json.dumps(REPORT, indent=2) + '\n')
        print(json.dumps(REPORT, indent=2), flush=True)
        for log in sorted(OUT.glob('collector-*.log')):
            print(log.name, log.read_text()[-12000:], flush=True)
        WORK.cleanup()
