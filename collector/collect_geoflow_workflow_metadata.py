#!/usr/bin/env python3
"""Collect VIVO-ready metadata for one Geoflow Nextflow run on Kubernetes."""

import argparse
import csv
import hashlib
import io
import json
import math
import os
import re
import shlex
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo


PROM_URL_DEFAULT = "http://127.0.0.1:19090"
CARBON_INTENSITY_DEFAULT = 0.4
BASE_URI_DEFAULT = "http://example.org/vivo-import/run-metadata/"
ONTOLOGY_URI_DEFAULT = "http://example.org/ontology/run-metadata#"

DEFAULT_NAMESPACE = "geoflow"
DEFAULT_DRIVER_POD = "nextflow-driver"
DEFAULT_WORKFLOW_NAME = "Geoflow - annual land-cover mapping across Germany"
DEFAULT_WORKFLOW_URI = (
    "http://example.org/vivo-import/run-metadata/workflow/"
    "geoflow-annual-land-cover-mapping-across-germany"
)
DEFAULT_PUBLICATION_URI = (
    "http://141.20.184.157:8080/vivo/individual/"
    "a1-publication-doi-10-13140-rg-2-2-25203-60963"
)
DEFAULT_CODE_URI = (
    "https://github.com/YagmurKati/geoflow-kubernetes-vivo-metadata"
)
DEFAULT_TRACE_ARCHIVE = "https://box.hu-berlin.de/d/083e70a0414846a5ae77/"
DEFAULT_APPLICATION_DOMAIN_URI = "http://172.28.33.178:8080/vivo/individual/n5261"
DEFAULT_RUN_OPERATOR_URI = "https://fonda.hu-berlin.de/?page_id=2066#YagmurKati"
DEFAULT_RESPONSIBLE_RESEARCHERS = ["Florian Katerndahl", "Dirk Pflugmacher"]
DEFAULT_RESPONSIBLE_RESEARCHER_URIS = [
    "https://fonda.hu-berlin.de/?page_id=2066#FelixKummer"
]
DEFAULT_SUBPROJECT_URIS = [
    "http://141.20.184.157:8080/vivo/individual/"
    "fonda-group-b5-transparent-multi-site-data-analysis-workflows-for-earth-observation"
]
DEFAULT_LANGUAGE_URIS = [
    "http://example.org/vivo-import/run-metadata/language/python",
    "http://example.org/vivo-import/run-metadata/language/shell",
]
DEFAULT_LANGUAGE_LABELS = {
    "http://example.org/vivo-import/run-metadata/language/python": "Python",
    "http://example.org/vivo-import/run-metadata/language/shell": "Shell",
}
DEFAULT_CLUSTER_URI = "http://example.org/vivo-import/run-metadata/cluster/fonda-cluster"
DEFAULT_ENGINE_URI = "http://example.org/vivo-import/run-metadata/engine/nextflow"
DEFAULT_BACKEND_URI = "http://172.28.33.178:8080/vivo/individual/n5703"
DEFAULT_OUTPUT_STEM = "geoflow-annual-land-cover-mapping-workflow-public-metadata"
DEFAULT_TRACE_TYPES = "Nextflow trace, execution report, timeline, and execution logs"
DEFAULT_TRACE_DATA_FORMAT = "TSV, HTML, and plain text"

BERLIN_TZ = ZoneInfo("Europe/Berlin")
MONTHS = {
    "Jan": 1,
    "Feb": 2,
    "Mar": 3,
    "Apr": 4,
    "May": 5,
    "Jun": 6,
    "Jul": 7,
    "Aug": 8,
    "Sep": 9,
    "Oct": 10,
    "Nov": 11,
    "Dec": 12,
}
LOG_TS_RE = re.compile(
    r"^(?P<month>[A-Z][a-z]{2})-(?P<day>\d{2}) "
    r"(?P<time>\d{2}:\d{2}:\d{2}\.\d+)"
)
SESSION_RE = re.compile(r"Session UUID:\s*(\S+)")
RUN_NAME_RE = re.compile(r"Run name:\s*(\S+)")
NEXTFLOW_VERSION_RE = re.compile(r"N E X T F L O W\s+~\s+version\s+(\S+)")
CONTAINER_RE = re.compile(r"(?m)^\s*container\s*=\s*['\"]([^'\"]+)['\"]")


@dataclass
class TaskRecord:
    task_id: str
    hash_value: str
    pod_name: Optional[str]
    name: str
    status: str
    exit_code: Optional[int]
    submit: datetime
    duration_seconds: Optional[float]
    realtime_seconds: Optional[float]
    end: Optional[datetime] = None


@dataclass
class PodMetrics:
    pod_name: str
    cpu_seconds: Optional[float] = None
    cpu_query: Optional[str] = None
    cpu_method: Optional[str] = None
    energy_joules: Optional[float] = None
    energy_query: Optional[str] = None
    energy_method: Optional[str] = None
    energy_estimated: bool = False
    memory_series: Dict[int, float] = field(default_factory=dict)
    memory_query: Optional[str] = None
    node_name: Optional[str] = None
    images: List[str] = field(default_factory=list)


@dataclass
class CarbonIntensityInfo:
    kg_per_kwh: float
    source: str
    source_uri: Optional[str] = None
    zone: Optional[str] = None
    point_count: int = 0
    includes_estimates: bool = False
    start: Optional[str] = None
    end: Optional[str] = None
    emissions_basis: str = "CO2e"
    temporal_granularity: Optional[str] = None


def run_cmd(args: Sequence[str], check: bool = True) -> str:
    proc = subprocess.run(args, text=True, capture_output=True)
    if check and proc.returncode != 0:
        command = " ".join(shlex.quote(part) for part in args)
        raise RuntimeError(f"Command failed: {command}\n{proc.stderr.strip()}")
    return proc.stdout


def kubectl_json(args: Sequence[str]) -> Dict[str, Any]:
    output = run_cmd(["kubectl", *args, "-o", "json"])
    return json.loads(output)


def remote_read(namespace: str, pod: str, path: str) -> str:
    script = f"test -r {shlex.quote(path)} && cat {shlex.quote(path)}"
    return run_cmd(["kubectl", "-n", namespace, "exec", pod, "--", "sh", "-c", script])


def detect_latest_remote_trace(namespace: str, pod: str) -> str:
    script = (
        "ls -1t /workspace/results/trace*.txt 2>/dev/null "
        "| head -n 1"
    )
    path = run_cmd(
        ["kubectl", "-n", namespace, "exec", pod, "--", "sh", "-c", script]
    ).strip()
    if not path:
        raise RuntimeError("No trace*.txt file found under /workspace/results")
    return path


def load_text(
    local_path: Optional[str],
    remote_path: Optional[str],
    namespace: str,
    driver_pod: str,
    required: bool,
) -> str:
    if local_path:
        return Path(local_path).read_text(encoding="utf-8")
    if remote_path:
        try:
            return remote_read(namespace, driver_pod, remote_path)
        except Exception:
            if required:
                raise
    return ""


def parse_duration(value: Optional[str]) -> Optional[float]:
    text = (value or "").strip()
    if not text or text == "-":
        return None
    total = 0.0
    matched = False
    for number, unit in re.findall(
        r"([0-9]+(?:\.[0-9]+)?)\s*(ms|us|ns|d|h|m|s)", text
    ):
        matched = True
        amount = float(number)
        multipliers = {
            "d": 86400.0,
            "h": 3600.0,
            "m": 60.0,
            "s": 1.0,
            "ms": 0.001,
            "us": 0.000001,
            "ns": 0.000000001,
        }
        total += amount * multipliers[unit]
    return total if matched else None


def parse_optional_int(value: Optional[str]) -> Optional[int]:
    text = (value or "").strip()
    if not text or text == "-":
        return None
    try:
        return int(text)
    except ValueError:
        return None


def parse_trace_datetime(value: str, trace_tz: ZoneInfo) -> datetime:
    parsed = datetime.strptime(value.strip(), "%Y-%m-%d %H:%M:%S.%f")
    return parsed.replace(tzinfo=trace_tz)


def parse_trace(text: str, trace_tz: ZoneInfo) -> List[TaskRecord]:
    reader = csv.DictReader(io.StringIO(text), delimiter="\t")
    required = {"task_id", "hash", "native_id", "name", "status", "submit"}
    if not reader.fieldnames or not required.issubset(set(reader.fieldnames)):
        raise RuntimeError(
            "The Nextflow trace is missing required columns: "
            + ", ".join(sorted(required))
        )

    tasks: List[TaskRecord] = []
    for row in reader:
        submit_text = (row.get("submit") or "").strip()
        if not submit_text or submit_text == "-":
            continue
        duration = parse_duration(row.get("duration"))
        submit = parse_trace_datetime(submit_text, trace_tz)
        pod_name = (row.get("native_id") or "").strip()
        if not pod_name or pod_name == "-":
            pod_name = None
        task = TaskRecord(
            task_id=(row.get("task_id") or "").strip(),
            hash_value=(row.get("hash") or "").strip(),
            pod_name=pod_name,
            name=(row.get("name") or "").strip(),
            status=(row.get("status") or "UNKNOWN").strip().upper(),
            exit_code=parse_optional_int(row.get("exit")),
            submit=submit,
            duration_seconds=duration,
            realtime_seconds=parse_duration(row.get("realtime")),
        )
        if duration is not None:
            task.end = submit + timedelta(seconds=duration)
        tasks.append(task)

    if not tasks:
        raise RuntimeError("The selected Nextflow trace contains no submitted tasks")
    return tasks


def parse_log_timestamp(line: str, year: int, log_tz: ZoneInfo) -> Optional[datetime]:
    match = LOG_TS_RE.match(line)
    if not match:
        return None
    month = MONTHS.get(match.group("month"))
    if month is None:
        return None
    clock = datetime.strptime(match.group("time"), "%H:%M:%S.%f")
    return datetime(
        year,
        month,
        int(match.group("day")),
        clock.hour,
        clock.minute,
        clock.second,
        clock.microsecond,
        tzinfo=log_tz,
    )


def extract_failure_reason(*logs: str) -> Optional[str]:
    merged = "\n".join(log for log in logs if log)
    error_match = re.search(r"(?m)^ERROR\s+~\s+(.+)$", merged)
    cause_match = re.search(r"(?m)^Caused by:\s*\n\s*(.+)$", merged)
    parts: List[str] = []
    if error_match:
        parts.append(error_match.group(1).strip())
    if cause_match:
        cause = cause_match.group(1).strip()
        if cause not in parts:
            parts.append(cause)
    return ": ".join(parts) if parts else None


def parse_log_metadata(
    debug_log: str,
    console_log: str,
    trace_year: int,
    log_tz: ZoneInfo,
) -> Dict[str, Any]:
    timestamps = [
        ts
        for ts in (
            parse_log_timestamp(line, trace_year, log_tz)
            for line in debug_log.splitlines()
        )
        if ts is not None
    ]
    session_match = SESSION_RE.search(debug_log)
    run_name_match = RUN_NAME_RE.search(debug_log)
    version_match = NEXTFLOW_VERSION_RE.search(debug_log)
    return {
        "start": min(timestamps) if timestamps else None,
        "end": max(timestamps) if timestamps else None,
        "session_id": session_match.group(1) if session_match else None,
        "run_name": run_name_match.group(1) if run_name_match else None,
        "nextflow_version": version_match.group(1) if version_match else None,
        "finished": "Execution complete" in debug_log,
        "failure_reason": extract_failure_reason(debug_log, console_log),
    }


def normalize_finished_tasks(tasks: List[TaskRecord], run_finished: bool) -> None:
    if not run_finished:
        return
    for task in tasks:
        if task.status in {"RUNNING", "SUBMITTED", "NEW", "PENDING"}:
            task.status = "ABORTED"


def derive_status(tasks: Sequence[TaskRecord], run_finished: bool) -> str:
    statuses = {task.status for task in tasks}
    if statuses & {"FAILED", "ERROR"}:
        return "Failed"
    if statuses & {"RUNNING", "SUBMITTED", "NEW", "PENDING"}:
        return "Running"
    if not run_finished:
        return "Running"
    if statuses and statuses <= {"COMPLETED", "CACHED"}:
        return "Succeeded"
    if "ABORTED" in statuses:
        return "Aborted"
    return "Unknown"


def task_status_label(status: str) -> str:
    mapping = {
        "COMPLETED": "Succeeded",
        "CACHED": "Cached",
        "FAILED": "Failed",
        "ERROR": "Failed",
        "ABORTED": "Aborted",
        "RUNNING": "Running",
        "SUBMITTED": "Submitted",
        "PENDING": "Pending",
    }
    return mapping.get(status.upper(), status.title())


def seconds_between(start: datetime, end: datetime) -> int:
    return max(1, int(math.ceil((end - start).total_seconds())))


def resource_accounting_window(
    tasks: Sequence[TaskRecord],
    run_start: datetime,
    run_end: datetime,
    include_cached_origin_metrics: bool,
) -> Tuple[datetime, datetime]:
    if not include_cached_origin_metrics:
        return run_start, run_end
    trace_start = min(task.submit for task in tasks)
    trace_end = max((task.end or run_end) for task in tasks)
    return min(run_start, trace_start), max(run_end, trace_end)


def http_get_json(
    url: str,
    params: Optional[Dict[str, str]] = None,
    headers: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    if params:
        url = f"{url}?{urllib.parse.urlencode(params)}"
    request_headers = {
        "Accept": "application/json",
        "User-Agent": "geoflow-vivo-metadata-collector/1.0",
    }
    request_headers.update(headers or {})
    request = urllib.request.Request(url, headers=request_headers)
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"HTTP {exc.code} returned by {url}: {detail}"
        ) from exc


def api_datetime(value: datetime) -> str:
    return (
        value.astimezone(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def electricity_maps_points(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    for key in ("data", "history"):
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    if "value" in payload or "carbonIntensity" in payload:
        return [payload]
    return []


def parse_api_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def resolve_co2map_carbon_intensity(
    args: argparse.Namespace,
    start: datetime,
    end: datetime,
) -> CarbonIntensityInfo:
    status = args.co2map_data_status
    endpoint = (
        f"{args.co2map_api_url.rstrip('/')}/"
        f"ConsumptionIntensity{status.title()}/"
    )
    start_utc = start.astimezone(timezone.utc)
    end_utc = end.astimezone(timezone.utc)
    query_end = end_utc + timedelta(days=1)
    payload = http_get_json(
        endpoint,
        {
            "state": args.co2map_state,
            "country": args.co2map_country,
            "start": start_utc.date().isoformat(),
            "end": query_end.date().isoformat(),
        },
    )
    series = next(
        (
            value
            for key, value in payload.items()
            if "Consumption-based Intensity" in key
            and isinstance(value, list)
        ),
        [],
    )
    values: List[float] = []
    selected_times: List[datetime] = []
    for item in series:
        if not isinstance(item, list) or len(item) < 2:
            continue
        try:
            point_start = parse_api_datetime(str(item[0]))
            value = float(item[1])
        except (TypeError, ValueError):
            continue
        point_end = point_start + timedelta(hours=1)
        if (
            point_start < end_utc
            and point_end > start_utc
            and math.isfinite(value)
        ):
            selected_times.append(point_start)
            values.append(value)
    if not values:
        raise RuntimeError(
            "CO2Map.de returned no finite consumption-intensity values "
            f"overlapping {api_datetime(start_utc)} to "
            f"{api_datetime(end_utc)}. Preliminary data can be delayed."
        )

    average_g_per_kwh = sum(values) / len(values)
    selected_start = min(selected_times)
    selected_end = max(selected_times) + timedelta(hours=1)
    return CarbonIntensityInfo(
        kg_per_kwh=average_g_per_kwh / 1000.0,
        source=(
            "CO2Map.de "
            f"{status} consumption-based direct CO2 intensity "
            "(model-based)"
        ),
        source_uri=endpoint,
        zone=args.co2map_state,
        point_count=len(values),
        includes_estimates=True,
        start=api_datetime(selected_start),
        end=api_datetime(selected_end),
        emissions_basis="direct CO2",
        temporal_granularity="hourly",
    )


def resolve_carbon_intensity(
    args: argparse.Namespace,
    start: datetime,
    end: datetime,
) -> CarbonIntensityInfo:
    if args.carbon_intensity_source == "fixed":
        return CarbonIntensityInfo(
            kg_per_kwh=args.carbon_intensity,
            source="Fixed configured emissions factor",
        )

    if args.carbon_intensity_source == "co2map":
        return resolve_co2map_carbon_intensity(args, start, end)

    token = os.environ.get(args.electricity_maps_api_token_env)
    if not token:
        raise RuntimeError(
            "Electricity Maps was selected, but environment variable "
            f"{args.electricity_maps_api_token_env} is empty."
        )

    step_seconds = 300
    start_epoch = int(start.astimezone(timezone.utc).timestamp())
    end_epoch = int(end.astimezone(timezone.utc).timestamp())
    query_start_epoch = (start_epoch // step_seconds) * step_seconds
    query_end_epoch = (
        ((end_epoch + step_seconds - 1) // step_seconds) * step_seconds
    )
    if query_end_epoch <= query_start_epoch:
        query_end_epoch = query_start_epoch + step_seconds
    query_start = datetime.fromtimestamp(query_start_epoch, timezone.utc)
    query_end = datetime.fromtimestamp(query_end_epoch, timezone.utc)

    params = {
        "zone": args.electricity_maps_zone,
        "start": api_datetime(query_start),
        "end": api_datetime(query_end),
        "emissionFactorType": "lifecycle",
        "temporalGranularity": "5_minutes",
        "flowTraced": "true",
        "disableEstimations": (
            "true"
            if args.electricity_maps_disable_estimations
            else "false"
        ),
    }
    payload = http_get_json(
        args.electricity_maps_api_url,
        params,
        {"auth-token": token},
    )
    points = electricity_maps_points(payload)
    values: List[float] = []
    includes_estimates = False
    for point in points:
        raw_value = point.get("value", point.get("carbonIntensity"))
        try:
            value = float(raw_value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(value):
            values.append(value)
            includes_estimates = (
                includes_estimates or bool(point.get("isEstimated"))
            )
    if not values:
        raise RuntimeError(
            "Electricity Maps returned no finite carbon-intensity values "
            f"for zone {args.electricity_maps_zone} between "
            f"{api_datetime(query_start)} and {api_datetime(query_end)}."
        )
    if args.electricity_maps_disable_estimations and includes_estimates:
        raise RuntimeError(
            "Electricity Maps returned at least one estimated carbon-intensity "
            "point even though --electricity-maps-disable-estimations was set."
        )

    average_g_per_kwh = sum(values) / len(values)
    return CarbonIntensityInfo(
        kg_per_kwh=average_g_per_kwh / 1000.0,
        source=(
            "Electricity Maps historical flow-traced lifecycle carbon "
            "intensity"
        ),
        source_uri=args.electricity_maps_api_url,
        zone=args.electricity_maps_zone,
        point_count=len(values),
        includes_estimates=includes_estimates,
        start=api_datetime(query_start),
        end=api_datetime(query_end),
        emissions_basis="lifecycle CO2e",
        temporal_granularity="5 minutes",
    )


def prom_query(prom_url: str, query: str) -> Any:
    data = http_get_json(f"{prom_url.rstrip('/')}/api/v1/query", {"query": query})
    if data.get("status") != "success":
        raise RuntimeError(f"Prometheus query failed:\n{query}\n{data}")
    return data["data"]["result"]


def prom_query_range(
    prom_url: str,
    query: str,
    start: datetime,
    end: datetime,
    step_seconds: int,
) -> Any:
    params = {
        "query": query,
        "start": str(start.timestamp()),
        "end": str(end.timestamp()),
        "step": f"{step_seconds}s",
    }
    data = http_get_json(f"{prom_url.rstrip('/')}/api/v1/query_range", params)
    if data.get("status") != "success":
        raise RuntimeError(f"Prometheus range query failed:\n{query}\n{data}")
    return data["data"]["result"]


def first_finite_value(result: Any) -> Optional[float]:
    if not result:
        return None
    try:
        if (
            isinstance(result, list)
            and len(result) == 2
            and not isinstance(result[0], dict)
        ):
            value = float(result[1])
        else:
            value = float(result[0]["value"][1])
    except (KeyError, IndexError, TypeError, ValueError):
        return None
    return value if math.isfinite(value) else None


def prometheus_metric_names(prom_url: str) -> List[str]:
    data = http_get_json(f"{prom_url.rstrip('/')}/api/v1/label/__name__/values")
    if data.get("status") != "success":
        return []
    return [str(name) for name in data.get("data", [])]


def ensure_prometheus_reachable(prom_url: str) -> None:
    try:
        result = prom_query(prom_url, "1")
    except Exception as exc:
        raise RuntimeError(
            f"Prometheus is not reachable at {prom_url}. Start the port-forward first."
        ) from exc
    if first_finite_value(result) != 1.0:
        raise RuntimeError(f"Prometheus at {prom_url} returned an unexpected response")


def find_energy_metric(metric_names: Sequence[str]) -> Optional[str]:
    preferred = [
        "kepler_container_joules_total",
        "kepler_pod_joules_total",
        "kepler_container_package_joules_total",
        "kepler_pod_package_joules_total",
    ]
    for metric in preferred:
        if metric in metric_names:
            return metric
    for metric in metric_names:
        low = metric.lower()
        if "kepler" in low and "joule" in low:
            return metric
    return None


def range_expr(selector: str, start: datetime, end: datetime) -> str:
    return f"{selector}[{seconds_between(start, end)}s] @ {int(end.timestamp())}"


def cpu_selectors(namespace: str, pod_name: str) -> List[str]:
    return [
        (
            "container_cpu_usage_seconds_total"
            f'{{namespace="{namespace}",pod="{pod_name}",'
            'container!="",container!="POD",image!=""}'
        ),
        (
            "container_cpu_usage_seconds_total"
            f'{{namespace="{namespace}",pod="{pod_name}",'
            'container!="",container!="POD"}'
        ),
        (
            "container_cpu_usage_seconds_total"
            f'{{pod="{pod_name}",container!="",container!="POD",image!=""}}'
        ),
    ]


def query_cpu_for_pod(
    prom_url: str,
    namespace: str,
    pod_name: str,
    start: datetime,
    end: datetime,
) -> Tuple[Optional[float], Optional[str], Optional[str]]:
    zero_result: Tuple[Optional[float], Optional[str], Optional[str]] = (
        None,
        None,
        None,
    )
    for selector in cpu_selectors(namespace, pod_name):
        query = f"sum(increase({range_expr(selector, start, end)}))"
        try:
            value = first_finite_value(prom_query(prom_url, query))
        except Exception:
            continue
        if value is not None and value > 0:
            return value, query, "increase"
        if value == 0 and zero_result[0] is None:
            zero_result = (value, query, "increase")

    for selector in cpu_selectors(namespace, pod_name):
        query = f"sum(max_over_time({range_expr(selector, start, end)}))"
        try:
            value = first_finite_value(prom_query(prom_url, query))
        except Exception:
            continue
        if value is not None and value > 0:
            return value, query, "counter-maximum fallback"
    return zero_result


def energy_selectors(metric: str, namespace: str, pod_name: str) -> List[str]:
    return [
        f'{metric}{{namespace="{namespace}",pod_name="{pod_name}"}}',
        f'{metric}{{namespace="{namespace}",pod="{pod_name}"}}',
        f'{metric}{{container_namespace="{namespace}",pod_name="{pod_name}"}}',
        f'{metric}{{pod_namespace="{namespace}",pod_name="{pod_name}"}}',
        f'{metric}{{pod_name="{pod_name}"}}',
        f'{metric}{{pod="{pod_name}"}}',
    ]


def query_energy_for_pod(
    prom_url: str,
    metric: Optional[str],
    namespace: str,
    pod_name: str,
    start: datetime,
    end: datetime,
) -> Tuple[Optional[float], Optional[str], Optional[str], bool]:
    if not metric:
        return None, None, None, False

    selectors = energy_selectors(metric, namespace, pod_name)
    zero_result: Tuple[Optional[float], Optional[str], Optional[str], bool] = (
        None,
        None,
        None,
        False,
    )
    for selector in selectors:
        query = f"sum(increase({range_expr(selector, start, end)}))"
        try:
            value = first_finite_value(prom_query(prom_url, query))
        except Exception:
            continue
        if value is not None and value > 0:
            return value, query, "counter increase", False
        if value == 0 and zero_result[0] is None:
            zero_result = (value, query, "counter increase", False)

    for selector in selectors:
        ranged = range_expr(selector, start, end)
        query = f"sum(max_over_time({ranged}) - min_over_time({ranged}))"
        try:
            value = first_finite_value(prom_query(prom_url, query))
        except Exception:
            continue
        if value is not None and value > 0:
            return value, query, "observed counter range fallback", True

    for selector in selectors:
        query = f"sum(max_over_time({range_expr(selector, start, end)}))"
        try:
            value = first_finite_value(prom_query(prom_url, query))
        except Exception:
            continue
        if value is not None and value > 0:
            return value, query, "counter maximum estimate", True
    return zero_result


def memory_selectors(namespace: str, pod_name: str) -> List[str]:
    return [
        (
            "container_memory_working_set_bytes"
            f'{{namespace="{namespace}",pod="{pod_name}",'
            'container!="",container!="POD",image!=""}'
        ),
        (
            "container_memory_working_set_bytes"
            f'{{namespace="{namespace}",pod="{pod_name}",'
            'container!="",container!="POD"}'
        ),
        (
            "container_memory_working_set_bytes"
            f'{{pod="{pod_name}",container!="",container!="POD",image!=""}}'
        ),
    ]


def query_memory_for_pod(
    prom_url: str,
    namespace: str,
    pod_name: str,
    start: datetime,
    end: datetime,
    step_seconds: int,
) -> Tuple[Dict[int, float], Optional[str]]:
    for selector in memory_selectors(namespace, pod_name):
        query = f"sum({selector})"
        try:
            result = prom_query_range(
                prom_url, query, start, end, step_seconds
            )
        except Exception:
            continue
        if not result:
            continue
        values = result[0].get("values", [])
        series: Dict[int, float] = {}
        for timestamp, raw_value in values:
            try:
                value = float(raw_value)
            except (TypeError, ValueError):
                continue
            if math.isfinite(value):
                series[int(float(timestamp))] = value
        if series:
            return series, query
    return {}, None


def candidate_probe_times(
    task: TaskRecord,
    run_start: datetime,
    run_end: datetime,
) -> List[datetime]:
    task_end = task.end or run_end
    candidates = [
        task.submit + (task_end - task.submit) / 2,
        max(task.submit, task_end - timedelta(seconds=5)),
        task.submit + timedelta(seconds=30),
        run_end,
    ]
    result: List[datetime] = []
    for candidate in candidates:
        bounded = min(max(candidate, run_start), run_end)
        if bounded not in result:
            result.append(bounded)
    return result


def query_pod_info(
    prom_url: str,
    namespace: str,
    task: TaskRecord,
    run_start: datetime,
    run_end: datetime,
) -> Tuple[Optional[str], List[str]]:
    if not task.pod_name:
        return None, []
    node_name: Optional[str] = None
    images: List[str] = []
    for timestamp in candidate_probe_times(task, run_start, run_end):
        at_expr = f" @ {int(timestamp.timestamp())}"
        info_queries = [
            (
                "kube_pod_info"
                f'{{namespace="{namespace}",pod="{task.pod_name}"}}{at_expr}'
            ),
            f'kube_pod_info{{pod="{task.pod_name}"}}{at_expr}',
        ]
        for query in info_queries:
            try:
                result = prom_query(prom_url, query)
            except Exception:
                continue
            if result:
                node_name = result[0].get("metric", {}).get("node")
                break
        if node_name:
            break

    for timestamp in candidate_probe_times(task, run_start, run_end):
        at_expr = f" @ {int(timestamp.timestamp())}"
        image_queries = [
            (
                "kube_pod_container_info"
                f'{{namespace="{namespace}",pod="{task.pod_name}"}}{at_expr}'
            ),
            f'kube_pod_container_info{{pod="{task.pod_name}"}}{at_expr}',
        ]
        for query in image_queries:
            try:
                result = prom_query(prom_url, query)
            except Exception:
                continue
            for row in result:
                image = row.get("metric", {}).get("image")
                if image and image not in images:
                    images.append(image)
            if images:
                break
        if images:
            break
    return node_name, images


def unique(values: Iterable[Optional[str]]) -> List[str]:
    result: List[str] = []
    for value in values:
        if value and value not in result:
            result.append(value)
    return result


def collect_pod_metrics(
    args: argparse.Namespace,
    tasks: Sequence[TaskRecord],
    metric_start: datetime,
    metric_end: datetime,
    energy_metric: Optional[str],
) -> Dict[str, PodMetrics]:
    task_by_pod = {
        task.pod_name: task
        for task in tasks
        if task.pod_name is not None
        and (
            task.status != "CACHED"
            or args.include_cached_origin_metrics
        )
    }
    result: Dict[str, PodMetrics] = {}
    for index, (pod_name, task) in enumerate(task_by_pod.items(), start=1):
        print(f"Collecting Prometheus metrics for pod {index}/{len(task_by_pod)}: {pod_name}")
        cpu_value, cpu_query, cpu_method = query_cpu_for_pod(
            args.prom_url,
            args.namespace,
            pod_name,
            metric_start,
            metric_end,
        )
        energy_value, energy_query, energy_method, energy_estimated = (
            query_energy_for_pod(
                args.prom_url,
                energy_metric,
                args.namespace,
                pod_name,
                metric_start,
                metric_end,
            )
        )
        memory_series, memory_query = query_memory_for_pod(
            args.prom_url,
            args.namespace,
            pod_name,
            metric_start,
            metric_end,
            args.memory_step_seconds,
        )
        node_name, images = query_pod_info(
            args.prom_url,
            args.namespace,
            task,
            metric_start,
            metric_end,
        )
        result[pod_name] = PodMetrics(
            pod_name=pod_name,
            cpu_seconds=cpu_value,
            cpu_query=cpu_query,
            cpu_method=cpu_method,
            energy_joules=energy_value,
            energy_query=energy_query,
            energy_method=energy_method,
            energy_estimated=energy_estimated,
            memory_series=memory_series,
            memory_query=memory_query,
            node_name=node_name,
            images=images,
        )
    return result


def aggregate_memory(
    metrics: Iterable[PodMetrics],
    start: datetime,
    end: datetime,
) -> Tuple[Optional[float], Optional[float]]:
    start_epoch = int(start.timestamp())
    end_epoch = int(end.timestamp())
    totals: Dict[int, float] = {}
    for pod_metrics in metrics:
        for timestamp, value in pod_metrics.memory_series.items():
            if start_epoch <= timestamp <= end_epoch:
                totals[timestamp] = totals.get(timestamp, 0.0) + value
    if not totals:
        return None, None
    values = list(totals.values())
    return sum(values) / len(values), max(values)


def bytes_to_gb(value: Optional[float]) -> Optional[float]:
    return value / 1_000_000_000.0 if value is not None else None


def joules_to_kwh(value: Optional[float]) -> Optional[float]:
    return value / 3_600_000.0 if value is not None else None


def classify_stage(task_name: str) -> Tuple[str, str]:
    if task_name.startswith("calc_indices:"):
        return "spectral-index-calculation", "Spectral index calculation"
    if task_name.startswith("explode_base_files:"):
        return "base-band-extraction", "Base-band extraction"
    if task_name.startswith("build_vrt_stack:"):
        return "vrt-stack-construction", "VRT stack construction"
    base_name = re.sub(r"\s+\(\d+\)$", "", task_name).strip()
    label = base_name.replace(":", " / ").replace("_", " ")
    label = label[:1].upper() + label[1:] if label else "Workflow process"
    return slugify(base_name), label


def group_tasks(tasks: Sequence[TaskRecord]) -> List[Dict[str, Any]]:
    groups: Dict[str, Dict[str, Any]] = {}
    for task in tasks:
        slug, label = classify_stage(task.name)
        group = groups.setdefault(
            slug, {"slug": slug, "label": label, "tasks": []}
        )
        group["tasks"].append(task)
    order = [
        "spectral-index-calculation",
        "base-band-extraction",
        "vrt-stack-construction",
    ]
    return sorted(
        groups.values(),
        key=lambda group: (
            order.index(group["slug"]) if group["slug"] in order else len(order),
            group["slug"],
        ),
    )


def summarize_metrics(
    pod_names: Sequence[str],
    pod_metrics: Dict[str, PodMetrics],
    start: datetime,
    end: datetime,
    carbon_intensity: float,
) -> Dict[str, Any]:
    selected = [
        pod_metrics[pod_name]
        for pod_name in unique(pod_names)
        if pod_name in pod_metrics
    ]
    cpu_values = [
        metrics.cpu_seconds
        for metrics in selected
        if metrics.cpu_seconds is not None
    ]
    energy_values = [
        metrics.energy_joules
        for metrics in selected
        if metrics.energy_joules is not None
    ]
    memory_avg, memory_peak = aggregate_memory(selected, start, end)
    cpu_total = sum(cpu_values) if cpu_values else None
    energy_joules = sum(energy_values) if energy_values else None
    energy_kwh = joules_to_kwh(energy_joules)
    energy_estimated = any(metrics.energy_estimated for metrics in selected)
    cpu_fallbacks = sum(
        1
        for metrics in selected
        if metrics.cpu_method and "fallback" in metrics.cpu_method
    )
    return {
        "cpu_seconds": cpu_total,
        "cpu_pod_count": len(cpu_values),
        "cpu_fallback_count": cpu_fallbacks,
        "memory_avg_gb": bytes_to_gb(memory_avg),
        "memory_peak_gb": bytes_to_gb(memory_peak),
        "memory_pod_count": sum(bool(metrics.memory_series) for metrics in selected),
        "energy_kwh": energy_kwh,
        "energy_pod_count": len(energy_values),
        "energy_estimated": energy_estimated,
        "carbon_kg": (
            energy_kwh * carbon_intensity
            if energy_kwh is not None
            else None
        ),
    }


def metric_methods(
    summary: Dict[str, Any],
    pod_count: int,
    carbon_info: CarbonIntensityInfo,
    cached_origin_metrics: bool = False,
) -> Dict[str, str]:
    cpu_method = (
        "CPU time is the sum of Prometheus "
        "container_cpu_usage_seconds_total counter increases"
        f" for {summary['cpu_pod_count']} of {pod_count} pod(s)."
    )
    if summary["cpu_fallback_count"]:
        cpu_method += (
            f" A counter-maximum fallback was used for "
            f"{summary['cpu_fallback_count']} short-lived pod(s)."
        )
    memory_method = (
        "Memory is derived from Prometheus "
        "container_memory_working_set_bytes. Values are summed across "
        "concurrent workflow pods at each sample; the reported average and "
        "peak are calculated from that aggregate time series."
    )
    energy_method = (
        f"Energy is the sum of per-pod Kepler measurements for "
        f"{summary['energy_pod_count']} of {pod_count} pod(s)."
    )
    if summary["energy_estimated"]:
        energy_method += (
            " At least one short-lived pod required a counter-range or "
            "counter-maximum estimate because a reliable counter increase "
            "was unavailable."
        )
    carbon_method = (
        "Carbon emissions are calculated using the emissions factor method: "
        "energy consumption (kWh) multiplied by "
        f"{carbon_info.kg_per_kwh} kg/kWh "
        f"({carbon_info.emissions_basis}) from "
        f"{carbon_info.source}."
    )
    if carbon_info.point_count:
        carbon_method += (
            f" The factor is the arithmetic mean of "
            f"{carbon_info.point_count} uniformly spaced carbon-intensity "
            "measurements over the resource-accounting window."
        )
    if cached_origin_metrics:
        cache_note = (
            " Metrics include the original Kubernetes pods that produced "
            "outputs reused from the Nextflow cache."
        )
        cpu_method += cache_note
        memory_method += cache_note
        energy_method += cache_note
    return {
        "cpu": cpu_method,
        "memory": memory_method,
        "energy": energy_method,
        "carbon": carbon_method,
    }


def parse_k8s_bytes(quantity: str) -> Optional[float]:
    match = re.fullmatch(
        r"([0-9]+(?:\.[0-9]+)?)([A-Za-z]+)?", (quantity or "").strip()
    )
    if not match:
        return None
    value = float(match.group(1))
    suffix = match.group(2) or ""
    multipliers = {
        "": 1.0,
        "Ki": 1024.0,
        "Mi": 1024.0 ** 2,
        "Gi": 1024.0 ** 3,
        "Ti": 1024.0 ** 4,
        "K": 1000.0,
        "M": 1000.0 ** 2,
        "G": 1000.0 ** 3,
        "T": 1000.0 ** 4,
    }
    multiplier = multipliers.get(suffix)
    return value * multiplier if multiplier is not None else None


def collect_node_info(node_names: Sequence[str]) -> List[Dict[str, Any]]:
    result: List[Dict[str, Any]] = []
    for node_name in node_names:
        try:
            node = kubectl_json(["get", "node", node_name])
        except Exception as exc:
            print(
                f"WARNING: could not read Kubernetes node {node_name}: {exc}",
                file=sys.stderr,
            )
            continue
        result.append(
            {
                "name": node_name,
                "node_info": node.get("status", {}).get("nodeInfo", {}),
                "allocatable": node.get("status", {}).get("allocatable", {}),
                "labels": node.get("metadata", {}).get("labels", {}),
            }
        )
    return result


def source_files(path: Path) -> List[Path]:
    if path.is_file():
        return [path]
    allowed_suffixes = {
        ".nf",
        ".config",
        ".groovy",
        ".py",
        ".sh",
        ".json",
        ".yaml",
        ".yml",
    }
    allowed_names = {"Dockerfile", "environment.yml"}
    ignored_dirs = {
        ".git",
        ".nextflow",
        "__pycache__",
        "results",
        "work",
        "vivo_metadata",
    }
    files: List[Path] = []
    for candidate in path.rglob("*"):
        if not candidate.is_file():
            continue
        if any(part in ignored_dirs for part in candidate.relative_to(path).parts):
            continue
        if candidate.suffix in allowed_suffixes or candidate.name in allowed_names:
            files.append(candidate)
    return sorted(files, key=lambda item: item.relative_to(path).as_posix())


def sha256_source(path: Path) -> str:
    if not path.exists():
        raise RuntimeError(f"Code path does not exist: {path}")
    digest = hashlib.sha256()
    root = path if path.is_dir() else path.parent
    files = source_files(path)
    if not files:
        raise RuntimeError(f"No source files found under {path}")
    for source_file in files:
        relative = source_file.relative_to(root).as_posix()
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        with source_file.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        digest.update(b"\0")
    return digest.hexdigest()


def git_metadata(code_path: Path) -> Tuple[Optional[str], Optional[bool]]:
    cwd = code_path if code_path.is_dir() else code_path.parent
    try:
        commit = run_cmd(["git", "-C", str(cwd), "rev-parse", "HEAD"]).strip()
        dirty = bool(
            run_cmd(["git", "-C", str(cwd), "status", "--porcelain"]).strip()
        )
        return commit or None, dirty
    except Exception:
        return None, None


def images_from_code(code_path: Path) -> List[str]:
    result: List[str] = []
    for source_file in source_files(code_path):
        if source_file.suffix != ".config":
            continue
        text = source_file.read_text(encoding="utf-8", errors="replace")
        for image in CONTAINER_RE.findall(text):
            if image not in result:
                result.append(image)
    return result


def slugify(text: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
    return slug or "item"


def ttl_escape(value: str) -> str:
    return (
        value.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\r", "\\r")
        .replace("\n", "\\n")
    )


def ttl_literal(value: Any, datatype: Optional[str] = None) -> str:
    if isinstance(value, bool):
        return f"\"{'true' if value else 'false'}\"^^xsd:boolean"
    if isinstance(value, int):
        return f'"{value}"^^{datatype or "xsd:integer"}'
    if isinstance(value, float):
        return f'"{repr(value)}"^^{datatype or "xsd:double"}'
    literal = f'"{ttl_escape(str(value))}"'
    return f"{literal}^^{datatype}" if datatype else literal


def ttl_uri(value: str) -> str:
    return f"<{value}>"


def ttl_label(value: str) -> str:
    return f"{ttl_literal(value)}@en"


def add_resource(
    lines: List[str],
    subject: str,
    predicates: Sequence[Tuple[str, str]],
) -> None:
    lines.append(subject)
    for index, (predicate, obj) in enumerate(predicates):
        punctuation = "." if index == len(predicates) - 1 else ";"
        lines.append(f"  {predicate} {obj} {punctuation}")
    lines.append("")


def local_naive_iso(value: datetime) -> str:
    return (
        value.astimezone(BERLIN_TZ)
        .replace(tzinfo=None, microsecond=0)
        .isoformat()
    )


def format_run_title(start: datetime, end: datetime) -> str:
    start_local = start.astimezone(BERLIN_TZ)
    end_local = end.astimezone(BERLIN_TZ)
    timezone_name = start_local.tzname() or "Europe/Berlin"
    if start_local.date() == end_local.date():
        return (
            f"run {start_local.strftime('%Y-%m-%d %H:%M')}"
            f"\N{EN DASH}{end_local.strftime('%H:%M')} {timezone_name}"
        )
    return (
        f"run {start_local.strftime('%Y-%m-%d %H:%M')}"
        f"\N{EN DASH}{end_local.strftime('%Y-%m-%d %H:%M')} {timezone_name}"
    )


def append_metric_predicates(
    predicates: List[Tuple[str, str]],
    summary: Dict[str, Any],
    methods: Dict[str, str],
    energy_metric: Optional[str],
    carbon_info: CarbonIntensityInfo,
) -> None:
    if summary["cpu_seconds"] is not None:
        predicates.append(
            ("rm:cpuTimeSeconds", ttl_literal(summary["cpu_seconds"]))
        )
    predicates.append(
        ("rm:cpuTimeCalculationMethod", ttl_literal(methods["cpu"]))
    )
    if summary["memory_peak_gb"] is not None:
        predicates.append(
            ("rm:memoryPeakGB", ttl_literal(summary["memory_peak_gb"]))
        )
    if summary["memory_avg_gb"] is not None:
        predicates.append(
            ("rm:memoryAvgGB", ttl_literal(summary["memory_avg_gb"]))
        )
    predicates.append(
        ("rm:memoryCalculationMethod", ttl_literal(methods["memory"]))
    )
    if summary["energy_kwh"] is not None:
        predicates.append(
            ("rm:energyKWh", ttl_literal(summary["energy_kwh"]))
        )
    if summary["carbon_kg"] is not None:
        predicates.append(
            ("rm:carbonEmissionKgCO2e", ttl_literal(summary["carbon_kg"]))
        )
    predicates.append(
        (
            "rm:carbonIntensityKgCO2ePerKWh",
            ttl_literal(carbon_info.kg_per_kwh),
        )
    )
    # Keep the established VIVO display predicate alongside the general one.
    predicates.append(
        (
            "rm:carbonIntensityAssumptionKgCO2ePerKWh",
            ttl_literal(carbon_info.kg_per_kwh),
        )
    )
    predicates.extend(
        [
            (
                "rm:carbonIntensitySource",
                ttl_literal(carbon_info.source),
            ),
            (
                "rm:energyMetricSource",
                ttl_literal(energy_metric or "none"),
            ),
            (
                "rm:energyCalculationMethod",
                ttl_literal(methods["energy"]),
            ),
            (
                "rm:energyCalculationUsesFallbackEstimate",
                ttl_literal(bool(summary["energy_estimated"])),
            ),
            (
                "rm:carbonCalculationMethod",
                ttl_literal(methods["carbon"]),
            ),
        ]
    )
    if carbon_info.source_uri:
        predicates.append(
            (
                "rm:carbonIntensitySourceLink",
                ttl_literal(carbon_info.source_uri, "xsd:anyURI"),
            )
        )
    if carbon_info.zone:
        predicates.append(
            ("rm:carbonIntensityZone", ttl_literal(carbon_info.zone))
        )
    if carbon_info.point_count:
        predicates.append(
            (
                "rm:carbonIntensityDataPointCount",
                ttl_literal(carbon_info.point_count, "xsd:integer"),
            )
        )
    if carbon_info.start:
        predicates.append(
            (
                "rm:carbonIntensityWindowStart",
                ttl_literal(carbon_info.start, "xsd:dateTime"),
            )
        )
    if carbon_info.end:
        predicates.append(
            (
                "rm:carbonIntensityWindowEnd",
                ttl_literal(carbon_info.end, "xsd:dateTime"),
            )
        )
    predicates.append(
        (
            "rm:carbonIntensityIncludesEstimatedData",
            ttl_literal(carbon_info.includes_estimates),
        )
    )
    predicates.append(
        (
            "rm:carbonIntensityEmissionsBasis",
            ttl_literal(carbon_info.emissions_basis),
        )
    )
    if carbon_info.temporal_granularity:
        predicates.append(
            (
                "rm:carbonIntensityTemporalGranularity",
                ttl_literal(carbon_info.temporal_granularity),
            )
        )


def build_ttl(
    args: argparse.Namespace,
    tasks: Sequence[TaskRecord],
    stages: Sequence[Dict[str, Any]],
    pod_metrics: Dict[str, PodMetrics],
    run_start: datetime,
    run_end: datetime,
    run_status: str,
    log_metadata: Dict[str, Any],
    code_version: str,
    git_commit: Optional[str],
    git_dirty: Optional[bool],
    energy_metric: Optional[str],
    carbon_info: CarbonIntensityInfo,
    node_infos: Sequence[Dict[str, Any]],
    images: Sequence[str],
    responsible_researchers: Sequence[str],
    responsible_researcher_uris: Sequence[str],
    subproject_uris: Sequence[str],
    language_uris: Sequence[str],
) -> Tuple[str, Dict[str, Any]]:
    base_uri = args.base_uri.rstrip("/") + "/"
    ontology_uri = (
        args.ontology_uri
        if args.ontology_uri.endswith(("#", "/"))
        else args.ontology_uri + "#"
    )
    session_key = (
        log_metadata.get("session_id")
        or f"{run_start.isoformat()}-{run_end.isoformat()}"
    )
    run_slug = slugify(
        f"{args.namespace}-{args.workflow_name}-{session_key}-{run_start.isoformat()}"
    )
    run_uri = f"{base_uri}run/{run_slug}"
    datetime_uri = f"{base_uri}datetime/{run_slug}"
    workflow_uri = args.workflow_uri
    engine_uri = args.engine_uri
    cluster_uri = args.cluster_uri
    publication_uri = args.publication_uri
    run_title = (
        f"{args.workflow_name} \N{MIDDLE DOT} "
        f"{format_run_title(run_start, run_end)}"
    )
    accounting_start, accounting_end = resource_accounting_window(
        tasks,
        run_start,
        run_end,
        args.include_cached_origin_metrics,
    )

    all_pod_names = unique(
        task.pod_name
        for task in tasks
        if (
            task.status != "CACHED"
            or args.include_cached_origin_metrics
        )
    )
    overall = summarize_metrics(
        all_pod_names,
        pod_metrics,
        accounting_start,
        accounting_end,
        carbon_info.kg_per_kwh,
    )
    overall_methods = metric_methods(
        overall,
        len(all_pod_names),
        carbon_info,
        args.include_cached_origin_metrics,
    )
    status_counts = Counter(task.status for task in tasks)
    node_names = unique(
        metrics.node_name for metrics in pod_metrics.values()
    )

    stage_records: List[Dict[str, Any]] = []
    for stage in stages:
        stage_tasks: List[TaskRecord] = stage["tasks"]
        executed_stage_tasks = [
            task for task in stage_tasks if task.status != "CACHED"
        ]
        cached_only = not executed_stage_tasks
        if cached_only and not args.include_cached_origin_metrics:
            stage_start = run_start
            stage_end = run_start
        else:
            timed_stage_tasks = executed_stage_tasks or stage_tasks
            stage_start = min(task.submit for task in timed_stage_tasks)
            stage_end = max(
                (task.end or run_end) for task in timed_stage_tasks
            )
        stage_status = derive_status(stage_tasks, True)
        stage_pods = unique(
            task.pod_name
            for task in stage_tasks
            if (
                task.status != "CACHED"
                or args.include_cached_origin_metrics
            )
        )
        stage_summary = summarize_metrics(
            stage_pods,
            pod_metrics,
            stage_start,
            stage_end,
            carbon_info.kg_per_kwh,
        )
        stage_methods = metric_methods(
            stage_summary,
            len(stage_pods),
            carbon_info,
            cached_only and args.include_cached_origin_metrics,
        )
        stage_key = f"{args.workflow_name}-{stage['slug']}-{session_key}"
        stage_uri = f"{base_uri}process/{slugify(stage_key)}"
        stage_records.append(
            {
                "uri": stage_uri,
                "slug": stage["slug"],
                "label": stage["label"],
                "tasks": stage_tasks,
                "pod_names": stage_pods,
                "start": stage_start,
                "end": stage_end,
                "status": stage_status,
                "cached_only": cached_only,
                "cached_origin_metrics": (
                    cached_only and args.include_cached_origin_metrics
                ),
                "summary": stage_summary,
                "methods": stage_methods,
            }
        )

    lines = [
        "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .",
        "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .",
        "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .",
        "@prefix dcterms: <http://purl.org/dc/terms/> .",
        "@prefix vivo: <http://vivoweb.org/ontology/core#> .",
        "@prefix bibo: <http://purl.org/ontology/bibo/> .",
        "@prefix prov: <http://www.w3.org/ns/prov#> .",
        f"@prefix rm: <{ontology_uri}> .",
        f"@prefix ex: <{base_uri}> .",
        "",
    ]

    add_resource(
        lines,
        "rm:WorkflowProcessRun",
        [
            ("rdf:type", "rdfs:Class"),
            ("rdfs:label", ttl_label("workflow stage run")),
        ],
    )
    add_resource(
        lines,
        "rm:hasWorkflowProcess",
        [
            ("rdf:type", "rdf:Property"),
            ("rdfs:label", ttl_label("workflow stages")),
        ],
    )
    add_resource(
        lines,
        "rm:isWorkflowProcessOf",
        [
            ("rdf:type", "rdf:Property"),
            ("rdfs:label", ttl_label("stage of workflow run")),
        ],
    )
    add_resource(
        lines,
        "rm:workflows",
        [
            ("rdf:type", "rdf:Property"),
            ("rdfs:label", ttl_label("workflows")),
        ],
    )
    add_resource(
        lines,
        "rm:hasWorkflow",
        [
            ("rdf:type", "rdf:Property"),
            ("rdfs:label", ttl_label("workflows")),
        ],
    )
    add_resource(
        lines,
        ttl_uri(engine_uri),
        [
            ("rdf:type", "rm:WorkflowEngine"),
            ("rdf:type", "vivo:InformationResource"),
            ("rdfs:label", ttl_label("Nextflow")),
            ("dcterms:title", ttl_label("Nextflow")),
        ],
    )
    for language_uri in language_uris:
        language_label = DEFAULT_LANGUAGE_LABELS.get(language_uri)
        if not language_label:
            continue
        add_resource(
            lines,
            ttl_uri(language_uri),
            [
                ("rdf:type", "rm:Language"),
                ("rdf:type", "vivo:InformationResource"),
                ("rdfs:label", ttl_label(language_label)),
                ("dcterms:title", ttl_label(language_label)),
            ],
        )
    cluster_predicates: List[Tuple[str, str]] = [
        ("rdf:type", "rm:ComputeCluster"),
        ("rdf:type", "vivo:InformationResource"),
        ("rdfs:label", ttl_label(args.cluster_label)),
        ("dcterms:title", ttl_label(args.cluster_label)),
        ("rm:hasRun", ttl_uri(run_uri)),
    ]
    add_resource(lines, ttl_uri(cluster_uri), cluster_predicates)

    workflow_predicates: List[Tuple[str, str]] = [
        ("rdf:type", "rm:Workflow"),
        ("rdf:type", "vivo:InformationResource"),
        ("rdf:type", "prov:Entity"),
        ("rdfs:label", ttl_label(args.workflow_name)),
        ("dcterms:title", ttl_label(args.workflow_name)),
        ("rm:workflowName", ttl_literal(args.workflow_name)),
        (
            "rm:workflowCodeLink",
            ttl_literal(args.code_uri, "xsd:anyURI"),
        ),
        (
            "rm:traceArchive",
            ttl_literal(args.trace_archive, "xsd:anyURI"),
        ),
        ("rm:traceTypes", ttl_literal(args.trace_types)),
        ("rm:traceDataFormat", ttl_literal(args.trace_data_format)),
        ("rm:workflowEngine", ttl_uri(engine_uri)),
        ("rm:applicationDomain", ttl_uri(args.application_domain_uri)),
    ]
    if publication_uri:
        workflow_predicates.append(
            ("rm:describesSoftwareExecution", ttl_uri(publication_uri))
        )
    for researcher in responsible_researchers:
        workflow_predicates.append(
            ("rm:responsibleResearcher", ttl_literal(researcher))
        )
    for researcher_uri in responsible_researcher_uris:
        workflow_predicates.append(
            ("rm:responsibleResearcher", ttl_uri(researcher_uri))
        )
    for subproject_uri in subproject_uris:
        workflow_predicates.append(("rm:subproject", ttl_uri(subproject_uri)))
    for language_uri in language_uris:
        workflow_predicates.append(("rm:language", ttl_uri(language_uri)))
    workflow_predicates.extend(
        [
            ("rm:hasRun", ttl_uri(run_uri)),
            ("rm:hasWorkflowRun", ttl_uri(run_uri)),
        ]
    )
    add_resource(lines, ttl_uri(workflow_uri), workflow_predicates)
    for researcher_uri in responsible_researcher_uris:
        add_resource(
            lines,
            ttl_uri(researcher_uri),
            [("rm:workflows", ttl_uri(workflow_uri))],
        )
    for subproject_uri in subproject_uris:
        add_resource(
            lines,
            ttl_uri(subproject_uri),
            [("rm:hasWorkflow", ttl_uri(workflow_uri))],
        )

    run_predicates: List[Tuple[str, str]] = [
        ("rdf:type", "rm:RunMetadata"),
        ("rdf:type", "vivo:InformationResource"),
        ("rdf:type", "prov:Entity"),
        ("rdfs:label", ttl_label(run_title)),
        ("dcterms:title", ttl_label(run_title)),
        ("rm:workflow", ttl_uri(workflow_uri)),
        ("rm:computeCluster", ttl_uri(cluster_uri)),
        ("rm:codeVersion", ttl_literal(f"sha256:{code_version}")),
        ("rm:runStatus", ttl_literal(run_status)),
        ("rm:workflowEngine", ttl_uri(engine_uri)),
        ("rm:backend", ttl_uri(args.backend_uri)),
        (
            "prov:startedAtTime",
            ttl_literal(local_naive_iso(run_start), "xsd:dateTime"),
        ),
        (
            "prov:endedAtTime",
            ttl_literal(local_naive_iso(run_end), "xsd:dateTime"),
        ),
        (
            "rm:startTime",
            ttl_literal(local_naive_iso(run_start), "xsd:dateTime"),
        ),
        (
            "rm:endTime",
            ttl_literal(local_naive_iso(run_end), "xsd:dateTime"),
        ),
        (
            "rm:durationSeconds",
            ttl_literal(seconds_between(run_start, run_end), "xsd:integer"),
        ),
        (
            "rm:durationCalculationMethod",
            ttl_literal(
                "Wall-clock time from the first to last timestamp in the "
                "Nextflow debug log, with trace timestamps as a fallback."
            ),
        ),
        (
            "dcterms:created",
            ttl_literal(
                datetime.now(BERLIN_TZ).replace(microsecond=0).isoformat(),
                "xsd:dateTime",
            ),
        ),
        (
            "vivo:dateTimeValue",
            ttl_uri(datetime_uri),
        ),
        (
            "rm:jobName",
            ttl_literal(log_metadata.get("run_name") or "geoflow"),
        ),
        (
            "rm:taskCount",
            ttl_literal(len(tasks), "xsd:integer"),
        ),
        (
            "rm:podCount",
            ttl_literal(len(all_pod_names), "xsd:integer"),
        ),
        (
            "rm:succeededTaskCount",
            ttl_literal(
                status_counts["COMPLETED"] + status_counts["CACHED"],
                "xsd:integer",
            ),
        ),
        (
            "rm:cachedTaskCount",
            ttl_literal(status_counts["CACHED"], "xsd:integer"),
        ),
        (
            "rm:failedTaskCount",
            ttl_literal(
                status_counts["FAILED"] + status_counts["ERROR"],
                "xsd:integer",
            ),
        ),
        (
            "rm:abortedTaskCount",
            ttl_literal(status_counts["ABORTED"], "xsd:integer"),
        ),
        (
            "rm:resourceAccountingScope",
            ttl_literal(
                (
                    "Current Nextflow execution plus origin pods for tasks "
                    "reused from the Nextflow cache."
                    if args.include_cached_origin_metrics
                    else "Pods executed by the current Nextflow execution."
                )
            ),
        ),
        (
            "rm:resourceAccountingStartTime",
            ttl_literal(
                local_naive_iso(accounting_start), "xsd:dateTime"
            ),
        ),
        (
            "rm:resourceAccountingEndTime",
            ttl_literal(
                local_naive_iso(accounting_end), "xsd:dateTime"
            ),
        ),
        (
            "rm:traceArchive",
            ttl_literal(args.trace_archive, "xsd:anyURI"),
        ),
        ("rm:traceTypes", ttl_literal(args.trace_types)),
        ("rm:traceDataFormat", ttl_literal(args.trace_data_format)),
    ]
    if publication_uri:
        run_predicates.append(
            ("rm:describesSoftwareExecution", ttl_uri(publication_uri))
        )
    if args.run_operator_uri:
        run_predicates.append(
            ("rm:runOperator", ttl_uri(args.run_operator_uri))
        )
    if log_metadata.get("session_id"):
        run_predicates.append(
            (
                "rm:nextflowSessionId",
                ttl_literal(log_metadata["session_id"]),
            )
        )
    if log_metadata.get("nextflow_version"):
        run_predicates.append(
            (
                "rm:nextflowVersion",
                ttl_literal(log_metadata["nextflow_version"]),
            )
        )
    if log_metadata.get("failure_reason"):
        run_predicates.append(
            (
                "rm:failureReason",
                ttl_literal(log_metadata["failure_reason"]),
            )
        )
    if git_commit:
        run_predicates.append(("rm:gitCommit", ttl_literal(git_commit)))
    if git_dirty is not None:
        run_predicates.append(("rm:codeModified", ttl_literal(git_dirty)))

    append_metric_predicates(
        run_predicates,
        overall,
        overall_methods,
        energy_metric,
        carbon_info,
    )
    for image in images:
        run_predicates.append(("rm:containerImage", ttl_literal(image)))
    for node_name in node_names:
        run_predicates.extend(
            [
                ("rm:executionHost", ttl_literal(node_name)),
                ("rm:nodeName", ttl_literal(node_name)),
            ]
        )

    for node in node_infos:
        node_info = node["node_info"]
        allocatable = node["allocatable"]
        if allocatable.get("cpu"):
            run_predicates.append(
                ("rm:allocatableCpu", ttl_literal(allocatable["cpu"]))
            )
        memory_bytes = parse_k8s_bytes(str(allocatable.get("memory", "")))
        if memory_bytes is not None:
            run_predicates.append(
                (
                    "rm:allocatableMemoryGB",
                    ttl_literal(bytes_to_gb(memory_bytes)),
                )
            )
        field_map = {
            "architecture": "rm:architecture",
            "kernelVersion": "rm:kernelVersion",
            "kubeletVersion": "rm:kubeletVersion",
            "osImage": "rm:osImage",
        }
        for key, predicate in field_map.items():
            if node_info.get(key):
                run_predicates.append(
                    (predicate, ttl_literal(node_info[key]))
                )

    run_predicates.extend(
        [
            ("rm:gpuRequested", ttl_literal(False)),
            ("rm:gpuMetricsAvailable", ttl_literal(False)),
            ("rm:gpuCapableNodeUsed", ttl_literal(False)),
            ("rm:gpuUsageStatus", ttl_literal("CPU-only workflow run")),
        ]
    )
    for stage in stage_records:
        run_predicates.append(
            ("rm:hasWorkflowProcess", ttl_uri(stage["uri"]))
        )
    add_resource(lines, ttl_uri(run_uri), run_predicates)

    if publication_uri:
        add_resource(
            lines,
            ttl_uri(publication_uri),
            [("rm:hasWorkflowRun", ttl_uri(run_uri))],
        )

    add_resource(
        lines,
        ttl_uri(datetime_uri),
        [
            ("rdf:type", "vivo:DateTimeValue"),
            (
                "vivo:dateTime",
                ttl_literal(local_naive_iso(run_start), "xsd:dateTime"),
            ),
            (
                "vivo:dateTimePrecision",
                "vivo:yearMonthDayTimePrecision",
            ),
        ],
    )

    for stage in stage_records:
        stage_status_counts = Counter(
            task.status for task in stage["tasks"]
        )
        predicates: List[Tuple[str, str]] = [
            ("rdf:type", "rm:WorkflowProcessRun"),
            ("rdf:type", "vivo:InformationResource"),
            ("rdf:type", "prov:Entity"),
            ("rdfs:label", ttl_label(stage["label"])),
            ("dcterms:title", ttl_label(stage["label"])),
            ("rm:jobName", ttl_literal(stage["label"])),
            ("rm:runStatus", ttl_literal(stage["status"])),
            (
                "rm:taskCount",
                ttl_literal(len(stage["tasks"]), "xsd:integer"),
            ),
            (
                "rm:podCount",
                ttl_literal(len(stage["pod_names"]), "xsd:integer"),
            ),
            (
                "rm:succeededTaskCount",
                ttl_literal(
                    stage_status_counts["COMPLETED"]
                    + stage_status_counts["CACHED"],
                    "xsd:integer",
                ),
            ),
            (
                "rm:cachedTaskCount",
                ttl_literal(
                    stage_status_counts["CACHED"], "xsd:integer"
                ),
            ),
            (
                "rm:failedTaskCount",
                ttl_literal(
                    stage_status_counts["FAILED"]
                    + stage_status_counts["ERROR"],
                    "xsd:integer",
                ),
            ),
            (
                "rm:abortedTaskCount",
                ttl_literal(
                    stage_status_counts["ABORTED"], "xsd:integer"
                ),
            ),
            (
                "prov:startedAtTime",
                ttl_literal(
                    local_naive_iso(stage["start"]), "xsd:dateTime"
                ),
            ),
            (
                "prov:endedAtTime",
                ttl_literal(
                    local_naive_iso(stage["end"]), "xsd:dateTime"
                ),
            ),
            (
                "rm:durationSeconds",
                ttl_literal(
                    (
                        0
                        if (
                            stage["cached_only"]
                            and not stage["cached_origin_metrics"]
                        )
                        else seconds_between(stage["start"], stage["end"])
                    ),
                    "xsd:integer",
                ),
            ),
            (
                "rm:durationCalculationMethod",
                ttl_literal(
                    (
                        (
                            "Original execution window of the Kubernetes "
                            "pods that produced outputs reused from the "
                            "Nextflow cache."
                        )
                        if stage["cached_origin_metrics"]
                        else (
                            "No new execution time: all tasks in this stage "
                            "were reused from the Nextflow cache."
                        )
                        if stage["cached_only"]
                        else
                        "Wall-clock time from the earliest task submission "
                        "to the latest task completion or workflow "
                        "termination in this stage."
                    )
                ),
            ),
            ("rm:isWorkflowProcessOf", ttl_uri(run_uri)),
        ]
        for task_name in unique(task.name for task in stage["tasks"]):
            predicates.append(("rm:taskName", ttl_literal(task_name)))
        append_metric_predicates(
            predicates,
            stage["summary"],
            stage["methods"],
            energy_metric,
            carbon_info,
        )
        predicates.append(
            (
                "rm:parallelismNote",
                ttl_literal(
                    "CPU time can exceed wall-clock duration because "
                    "Nextflow tasks execute concurrently."
                ),
            )
        )
        add_resource(lines, ttl_uri(stage["uri"]), predicates)

    audit = {
        "workflow_name": args.workflow_name,
        "workflow_uri": workflow_uri,
        "run_uri": run_uri,
        "run_status": run_status,
        "run_start_utc": run_start.astimezone(timezone.utc).isoformat(),
        "run_end_utc": run_end.astimezone(timezone.utc).isoformat(),
        "run_start_berlin": run_start.astimezone(BERLIN_TZ).isoformat(),
        "run_end_berlin": run_end.astimezone(BERLIN_TZ).isoformat(),
        "duration_seconds": seconds_between(run_start, run_end),
        "resource_accounting_start_utc": (
            accounting_start.astimezone(timezone.utc).isoformat()
        ),
        "resource_accounting_end_utc": (
            accounting_end.astimezone(timezone.utc).isoformat()
        ),
        "task_count": len(tasks),
        "executed_task_count": sum(
            task.status != "CACHED" for task in tasks
        ),
        "cached_task_count": status_counts["CACHED"],
        "pod_count": len(all_pod_names),
        "include_cached_origin_metrics": (
            args.include_cached_origin_metrics
        ),
        "nextflow": {
            "session_id": log_metadata.get("session_id"),
            "run_name": log_metadata.get("run_name"),
            "version": log_metadata.get("nextflow_version"),
            "failure_reason": log_metadata.get("failure_reason"),
        },
        "code": {
            "sha256": code_version,
            "git_commit": git_commit,
            "git_dirty": git_dirty,
            "uri": args.code_uri,
        },
        "prometheus": {
            "url": args.prom_url,
            "energy_metric": energy_metric,
        },
        "carbon_intensity": asdict(carbon_info),
        "summary": overall,
        "tasks": [
            {
                **asdict(task),
                "submit": task.submit.isoformat(),
                "end": task.end.isoformat() if task.end else None,
            }
            for task in tasks
        ],
        "pod_metrics": {
            pod_name: asdict(metrics)
            for pod_name, metrics in pod_metrics.items()
        },
        "stages": [
            {
                "slug": stage["slug"],
                "label": stage["label"],
                "uri": stage["uri"],
                "status": stage["status"],
                "task_count": len(stage["tasks"]),
                "executed_task_count": sum(
                    task.status != "CACHED" for task in stage["tasks"]
                ),
                "cached_task_count": sum(
                    task.status == "CACHED" for task in stage["tasks"]
                ),
                "pod_count": len(stage["pod_names"]),
                "cached_only": stage["cached_only"],
                "cached_origin_metrics": stage["cached_origin_metrics"],
                "start": stage["start"].isoformat(),
                "end": stage["end"].isoformat(),
                "summary": stage["summary"],
            }
            for stage in stage_records
        ],
        "nodes": list(node_infos),
        "images": list(images),
    }
    return "\n".join(lines).rstrip() + "\n", audit


def build_args() -> argparse.Namespace:
    here = Path(__file__).resolve().parent
    repository_root = here.parent
    default_code_path = repository_root / "runtime" / "geoflow"
    parser = argparse.ArgumentParser(
        description=(
            "Collect CPU, memory, Kepler energy, carbon, timing, Kubernetes, "
            "and Nextflow metadata for one Geoflow run and write VIVO-ready TTL."
        )
    )
    parser.add_argument("--namespace", default=DEFAULT_NAMESPACE)
    parser.add_argument("--driver-pod", default=DEFAULT_DRIVER_POD)
    parser.add_argument(
        "--trace-file",
        default=None,
        help="Local Nextflow trace TSV. By default the newest cluster trace is used.",
    )
    parser.add_argument(
        "--trace-remote",
        default=None,
        help="Explicit trace path inside nextflow-driver.",
    )
    parser.add_argument("--debug-log-file", default=None)
    parser.add_argument(
        "--debug-log-remote",
        default="/workspace/geoflow/.nextflow.log",
    )
    parser.add_argument("--console-log-file", default=None)
    parser.add_argument("--console-log-remote", default=None)
    parser.add_argument(
        "--trace-timezone",
        default="UTC",
        help="Timezone used by Nextflow trace and debug log timestamps.",
    )
    parser.add_argument("--workflow-name", default=DEFAULT_WORKFLOW_NAME)
    parser.add_argument("--workflow-uri", default=DEFAULT_WORKFLOW_URI)
    parser.add_argument("--publication-uri", default=DEFAULT_PUBLICATION_URI)
    parser.add_argument("--code-path", default=str(default_code_path))
    parser.add_argument("--code-uri", default=DEFAULT_CODE_URI)
    parser.add_argument("--trace-archive", default=DEFAULT_TRACE_ARCHIVE)
    parser.add_argument("--trace-types", default=DEFAULT_TRACE_TYPES)
    parser.add_argument("--trace-data-format", default=DEFAULT_TRACE_DATA_FORMAT)
    parser.add_argument(
        "--responsible-researcher",
        action="append",
        default=None,
        help="Repeat for each literal researcher name; defaults to Geoflow authors.",
    )
    parser.add_argument(
        "--responsible-researcher-uri",
        action="append",
        default=None,
        help="Repeat for each clickable researcher URI; defaults to Felix Kummer.",
    )
    parser.add_argument(
        "--subproject-uri",
        action="append",
        default=None,
        help="Repeat for each subproject URI; defaults to FONDA B5.",
    )
    parser.add_argument(
        "--language-uri",
        action="append",
        default=None,
        help="Repeat for each non-workflow-engine language URI; defaults to Python and Shell.",
    )
    parser.add_argument(
        "--application-domain-uri",
        default=DEFAULT_APPLICATION_DOMAIN_URI,
    )
    parser.add_argument("--run-operator-uri", default=DEFAULT_RUN_OPERATOR_URI)
    parser.add_argument("--cluster-uri", default=DEFAULT_CLUSTER_URI)
    parser.add_argument("--cluster-label", default="Fonda Cluster")
    parser.add_argument("--engine-uri", default=DEFAULT_ENGINE_URI)
    parser.add_argument("--backend-uri", default=DEFAULT_BACKEND_URI)
    parser.add_argument("--prom-url", default=PROM_URL_DEFAULT)
    parser.add_argument(
        "--carbon-intensity",
        type=float,
        default=CARBON_INTENSITY_DEFAULT,
        help="kg CO2e per kWh; default matches the earlier FORCE collector.",
    )
    parser.add_argument(
        "--carbon-intensity-source",
        choices=("fixed", "electricity-maps", "co2map"),
        default="fixed",
        help=(
            "Use the configured fixed factor or historical Electricity Maps "
            "or CO2Map.de data aligned to the resource-accounting window."
        ),
    )
    parser.add_argument(
        "--electricity-maps-api-token-env",
        default="ELECTRICITY_MAPS_API_TOKEN",
        help="Environment variable containing the Electricity Maps API token.",
    )
    parser.add_argument("--electricity-maps-zone", default="DE")
    parser.add_argument(
        "--electricity-maps-api-url",
        default=(
            "https://api.electricitymaps.com/v4/"
            "carbon-intensity/past-range"
        ),
    )
    parser.add_argument(
        "--electricity-maps-disable-estimations",
        action="store_true",
        help="Reject estimated Electricity Maps carbon-intensity points.",
    )
    parser.add_argument(
        "--co2map-api-url",
        default="https://api.co2map.de",
    )
    parser.add_argument("--co2map-state", default="DE")
    parser.add_argument("--co2map-country", default="DE")
    parser.add_argument(
        "--co2map-data-status",
        choices=("preliminary", "historical"),
        default="preliminary",
        help=(
            "Use preliminary near-real-time data or finalized historical "
            "CO2Map.de data."
        ),
    )
    parser.add_argument("--memory-step-seconds", type=int, default=15)
    parser.add_argument("--metrics-padding-seconds", type=int, default=30)
    parser.add_argument(
        "--include-cached-origin-metrics",
        action="store_true",
        help=(
            "Attribute metrics from the original pods referenced by CACHED "
            "trace rows. Use this for a composite Nextflow session view."
        ),
    )
    parser.add_argument("--base-uri", default=BASE_URI_DEFAULT)
    parser.add_argument("--ontology-uri", default=ONTOLOGY_URI_DEFAULT)
    parser.add_argument(
        "--output-dir",
        default=str(repository_root / "metadata" / "generated"),
    )
    parser.add_argument("--output-file", default=None)
    parser.add_argument("--output-stamp", default=None)
    parser.add_argument(
        "--allow-active-run",
        action="store_true",
        help="Allow collection while the trace still describes an active run.",
    )
    parser.add_argument(
        "--allow-missing-metrics",
        action="store_true",
        help="Write structural TTL even when Prometheus has no pod metrics.",
    )
    return parser.parse_args()


def main() -> None:
    args = build_args()
    trace_tz = ZoneInfo(args.trace_timezone)

    trace_remote = args.trace_remote
    if not args.trace_file and not trace_remote:
        trace_remote = detect_latest_remote_trace(
            args.namespace, args.driver_pod
        )
    trace_text = load_text(
        args.trace_file,
        trace_remote,
        args.namespace,
        args.driver_pod,
        required=True,
    )
    tasks = parse_trace(trace_text, trace_tz)

    debug_log = load_text(
        args.debug_log_file,
        args.debug_log_remote,
        args.namespace,
        args.driver_pod,
        required=False,
    )
    console_log = load_text(
        args.console_log_file,
        args.console_log_remote,
        args.namespace,
        args.driver_pod,
        required=False,
    )
    trace_start = min(task.submit for task in tasks)
    log_metadata = parse_log_metadata(
        debug_log,
        console_log,
        trace_start.year,
        trace_tz,
    )
    normalize_finished_tasks(tasks, bool(log_metadata["finished"]))

    run_start = log_metadata["start"] or trace_start
    derived_task_ends = [task.end for task in tasks if task.end is not None]
    trace_end = max(
        derived_task_ends + [max(task.submit for task in tasks)]
    )
    run_end = log_metadata["end"] or trace_end
    if run_end < run_start:
        raise RuntimeError("Derived run end precedes run start")
    for task in tasks:
        if task.end is None and log_metadata["finished"]:
            task.end = run_end

    run_status = derive_status(tasks, bool(log_metadata["finished"]))
    if run_status == "Running" and not args.allow_active_run:
        raise RuntimeError(
            "The selected trace still describes an active run. Wait for "
            "Nextflow to finish, or pass --allow-active-run explicitly."
        )

    ensure_prometheus_reachable(args.prom_url)
    metric_names = prometheus_metric_names(args.prom_url)
    energy_metric = find_energy_metric(metric_names)
    accounting_start, accounting_end = resource_accounting_window(
        tasks,
        run_start,
        run_end,
        args.include_cached_origin_metrics,
    )
    metric_start = accounting_start - timedelta(
        seconds=args.metrics_padding_seconds
    )
    metric_end = accounting_end + timedelta(
        seconds=args.metrics_padding_seconds
    )
    carbon_info = resolve_carbon_intensity(
        args,
        accounting_start,
        accounting_end,
    )
    pod_metrics = collect_pod_metrics(
        args, tasks, metric_start, metric_end, energy_metric
    )

    any_metrics = any(
        metrics.cpu_seconds is not None
        or metrics.energy_joules is not None
        or bool(metrics.memory_series)
        for metrics in pod_metrics.values()
    )
    if not any_metrics and not args.allow_missing_metrics:
        raise RuntimeError(
            "Prometheus returned no CPU, memory, or energy measurements for "
            "the pods in this trace. Check the port-forward and retention, or "
            "pass --allow-missing-metrics to emit structural metadata only."
        )

    code_path = Path(args.code_path).expanduser().resolve()
    code_version = sha256_source(code_path)
    git_commit, git_dirty = git_metadata(code_path)
    images = unique(
        image
        for metrics in pod_metrics.values()
        for image in metrics.images
    )
    if not images:
        images = images_from_code(code_path)
    node_names = unique(
        metrics.node_name for metrics in pod_metrics.values()
    )
    node_infos = collect_node_info(node_names)
    stages = group_tasks(tasks)
    responsible_researchers = (
        args.responsible_researcher
        if args.responsible_researcher is not None
        else DEFAULT_RESPONSIBLE_RESEARCHERS
    )
    responsible_researcher_uris = (
        args.responsible_researcher_uri
        if args.responsible_researcher_uri is not None
        else DEFAULT_RESPONSIBLE_RESEARCHER_URIS
    )
    subproject_uris = (
        args.subproject_uri
        if args.subproject_uri is not None
        else DEFAULT_SUBPROJECT_URIS
    )
    language_uris = (
        args.language_uri
        if args.language_uri is not None
        else DEFAULT_LANGUAGE_URIS
    )

    ttl_text, audit = build_ttl(
        args=args,
        tasks=tasks,
        stages=stages,
        pod_metrics=pod_metrics,
        run_start=run_start,
        run_end=run_end,
        run_status=run_status,
        log_metadata=log_metadata,
        code_version=code_version,
        git_commit=git_commit,
        git_dirty=git_dirty,
        energy_metric=energy_metric,
        carbon_info=carbon_info,
        node_infos=node_infos,
        images=images,
        responsible_researchers=responsible_researchers,
        responsible_researcher_uris=responsible_researcher_uris,
        subproject_uris=subproject_uris,
        language_uris=language_uris,
    )

    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = args.output_stamp or run_start.astimezone(BERLIN_TZ).strftime(
        "%d%m%Y_%H%M"
    )
    output_path = (
        Path(args.output_file).expanduser().resolve()
        if args.output_file
        else output_dir / f"{DEFAULT_OUTPUT_STEM}-{stamp}.ttl"
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(ttl_text, encoding="utf-8")
    audit_path = output_path.with_suffix(".metrics.json")
    audit_path.write_text(
        json.dumps(audit, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    manifest_path = output_path.parent / "VIVO_UPLOAD_FILES.txt"
    manifest_path.write_text(
        f"TTL files ready to upload to VIVO:\n{output_path}\n",
        encoding="utf-8",
    )

    summary = audit["summary"]
    print(f"Wrote TTL: {output_path}")
    print(f"Wrote audit JSON: {audit_path}")
    print(f"Wrote manifest: {manifest_path}")
    print(f"selected_trace={args.trace_file or trace_remote}")
    print(f"run_status={run_status}")
    print(f"task_count={len(tasks)}")
    print(f"pod_count={len(pod_metrics)}")
    print(f"workflow_start={run_start.isoformat()}")
    print(f"workflow_end={run_end.isoformat()}")
    print(f"cpu_seconds={summary['cpu_seconds']}")
    print(f"memory_avg_gb={summary['memory_avg_gb']}")
    print(f"memory_peak_gb={summary['memory_peak_gb']}")
    print(f"energy_metric={energy_metric or 'none'}")
    print(f"energy_kwh={summary['energy_kwh']}")
    print(f"carbon_intensity_source={carbon_info.source}")
    print(
        "carbon_intensity_kg_per_kwh="
        f"{carbon_info.kg_per_kwh}"
    )
    print(f"carbon_kg_co2e={summary['carbon_kg']}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Interrupted", file=sys.stderr)
        sys.exit(130)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
