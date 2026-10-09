"""A run report as Markdown, for the GitHub run page: status, source, one row per phase, totals and every problem."""

from typing import Any

STATUS_ICONS = {"SUCCESS": "✅", "PARTIAL": "⚠️", "FAILED": "❌", "SKIPPED": "⏭️"}
NOT_REACHED = "—"
MAX_LISTED = 10  # per kind of problem; the rest is counted


def summarize(report: dict[str, Any]) -> str:
    run = report["run"]
    status = run["status"]
    lines = [
        f"## {STATUS_ICONS.get(status, '❔')} {status} · run `{run['run_id']}` ({run['env']})",
        "",
        (
            f"Exit code {run['exit_code']} · {run['duration_seconds']:.1f} s · started {run['started_at']} · "
            f"host `{run['host']}`"
        ),
    ]
    if run.get("note"):
        lines += ["", f"> {run['note']}"]
    if run.get("error"):
        lines += ["", f"**Error:** `{run['error']['type']}: {_one_line(run['error']['message'])}`"]

    source = report.get("source")
    if source:
        pin = (
            "matches the pinned revision"
            if not source["tag_moved"]
            else f"⚠️ tag moved to `{source['label_resolves_to']}`"
        )
        lines += [
            "",
            f"**Source:** `{source['repo_id']}` @ `{source['revision']}` · pin `{source['revision_label']}`: {pin}",
        ]

    lines += ["", "| Phase | Result |", "|---|---|"]
    lines += [f"| {phase} | {text} |" for phase, text in _phase_rows(report)]

    totals = report.get("totals")
    if totals:
        lines += ["", f"**Totals:** {totals['landed_files_total']:,} files · {totals['rows_total']:,} rows"]

    problems = _problems(report)
    lines += ["", "### Problems", ""]
    lines += problems if problems else ["No problems."]
    commit = (report.get("code") or {}).get("commit")
    if commit:
        lines += ["", f"<sub>code `{commit[:12]}`</sub>"]
    return "\n".join(lines) + "\n"


def _phase_rows(report: dict[str, Any]) -> list[tuple[str, str]]:
    plan, fetch = report.get("plan"), report.get("fetch")
    ingest = NOT_REACHED
    if plan:
        ingest = f"{plan['new']:,} new, {plan['changed']:,} changed, {plan['unchanged']:,} unchanged"
        if fetch:
            ingest += (
                f" · {fetch['bytes']:,} bytes downloaded · {fetch.get('network_retries', 0)} network retries"
                f" · {fetch.get('checksum_redownloads', 0)} checksum re-downloads"
            )

    schema = report.get("schema")
    schema_text = NOT_REACHED
    if schema:
        schema_text = f"{len(schema['drifted'])} drifted of {schema['files']:,} inspected"
        schema_text += f" (fail_on_drift {'on' if schema['fail_on_drift'] else 'off'})"

    convert = report.get("convert")
    convert_text = f"{convert['converted']:,} converted, {convert['failed']:,} failed" if convert else NOT_REACHED
    if convert and convert.get("skipped"):
        convert_text += f", {convert['skipped']:,} skipped"

    prune = report.get("prune")
    prune_text = f"{len(prune['removed']):,} removed" if prune else NOT_REACHED

    reconcile = report.get("reconcile")
    reconcile_text = NOT_REACHED
    if reconcile:
        passed = sum(c["passed"] for c in reconcile["checks"])
        reconcile_text = (
            f"{passed}/{len(reconcile['checks'])} checks passed · {reconcile['mode']} · "
            f"{reconcile['spark_seconds']:.1f} s"
        )

    publish = report.get("publish")
    publish_text = NOT_REACHED
    if publish:
        publish_text = "marker written: yes" if publish["written"] else f"marker written: no ({publish['reason']})"

    return [
        ("ingest", ingest),
        ("schema", schema_text),
        ("convert", convert_text),
        ("prune", prune_text),
        ("reconcile", reconcile_text),
        ("publish", publish_text),
    ]


def _problems(report: dict[str, Any]) -> list[str]:
    problems: list[str] = []
    run = report["run"]
    if run.get("error"):
        problems.append(f"- **Run error:** `{run['error']['type']}: {_one_line(run['error']['message'])}`")
    if (report.get("source") or {}).get("tag_moved"):
        problems.append("- **Tag moved:** the run used the pinned revision; upstream changed the tag")

    failures = report.get("failures") or []
    problems += _listed("Failed files", [f"`{f['path']}`: {_one_line(f['error'])}" for f in failures])
    convert = report.get("convert") or {}
    problems += _listed(
        "Conversion failures", [f"`{f['path']}`: {_one_line(f['error'])}" for f in convert.get("failures", [])]
    )
    schema = report.get("schema") or {}
    problems += _listed("Schema drift", [f"`{d['path']}`: {_drift(d)}" for d in schema.get("drifted", [])])

    plan = report.get("plan") or {}
    problems += _listed("Rejected names", [f"`{r['path']}`: {r['reason']}" for r in plan.get("rejected_names", [])])
    problems += _listed("Removed upstream", [f"`{p}`" for p in plan.get("removed_upstream_paths", [])])

    reconcile = report.get("reconcile") or {}
    problems += _listed(
        "Failed reconcile checks",
        [
            f"check {c['number']} {c['name']}: {_check_detail(c)}"
            for c in reconcile.get("checks", [])
            if not c["passed"]
        ],
    )
    problems += _listed("Orphans in landing", [f"`{p}`" for p in report.get("orphans") or []])
    problems += _listed("Stale staging folders", [f"`{p}`" for p in report.get("stale_staging") or []])
    return problems


def _listed(title: str, items: list[str]) -> list[str]:
    if not items:
        return []
    lines = [f"- **{title}** ({len(items)}):"] + [f"  - {item}" for item in items[:MAX_LISTED]]
    if len(items) > MAX_LISTED:
        lines.append(f"  - … and {len(items) - MAX_LISTED} more (see the report)")
    return lines


def _drift(d: dict[str, Any]) -> str:
    if not d["readable"]:
        return f"unreadable ({_one_line(d.get('error') or '')})"
    parts = []
    if d["missing"]:
        parts.append(f"missing {d['missing']}")
    if d["extra"]:
        parts.append(f"extra {d['extra']}")
    if d["type_changes"]:
        parts.append("types " + ", ".join(f"{c}: {e} -> {a}" for c, e, a in d["type_changes"]))
    return "; ".join(parts)


def _check_detail(check: dict[str, Any]) -> str:
    details = {k: v for k, v in check["details"].items() if not isinstance(v, list | dict) and k != "shared_pass"}
    differences = check["details"].get("differences")
    if differences and isinstance(differences, list):
        details["differences"] = len(differences)
    return ", ".join(f"{k} {v}" for k, v in details.items()) or "see the report"


def _one_line(text: str, limit: int = 200) -> str:
    text = " ".join(str(text).split())
    return text if len(text) <= limit else text[: limit - 1] + "…"
