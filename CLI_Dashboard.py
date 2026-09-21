import sys
from typing import Optional
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.tree import Tree
from rich import box

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

from IngestionController import MetadataRepository
from MetadataServices import (
    ConstraintEngine,
    LineageAndRecEngine,
    StalenessAndEvolutionMonitor,
    ValidationSeverity,
)

console = Console(legacy_windows=False)


def display_catalog_banner():
    console.print(
        Panel(
            "[bold cyan]MeDOM & DLD Metadata Management System[/bold cyan]\n"
            "[dim]Ontological Data Lake Metadata Framework & Guided Pilot Engine (Figure 7)[/dim]",
            box=box.DOUBLE,
            style="bold blue",
        )
    )


def display_datasets_table(repo: MetadataRepository):
    table = Table(
        title="Registered DataSets (DLD & TMDObjects)",
        box=box.ROUNDED,
        header_style="bold magenta",
    )
    table.add_column("Dataset ID", style="cyan", no_wrap=True)
    table.add_column("Name", style="bold white")
    table.add_column("Structure", style="green")
    table.add_column("Stage", style="yellow")
    table.add_column("Volume (Bytes)", justify="right")
    table.add_column("Records", justify="right")
    table.add_column("Cols", justify="right")
    table.add_column("Veracity", justify="center")
    table.add_column("TV-Value", style="magenta")

    for ds_id, ds in repo.datasets.items():
        tmd = repo.tmd_objects.get(ds_id)
        byte_str = f"{tmd.volume.byte_size:,}" if tmd else "-"
        rec_str = f"{tmd.volume.record_count:,}" if tmd and tmd.volume.record_count is not None else "-"
        col_str = str(tmd.variety.column_count) if tmd else "-"
        ver_str = f"{tmd.veracity.quality_score:.2f}" if tmd else "-"
        val_str = tmd.value.business_criticality.value if (tmd and tmd.value) else ("N/A (Raw)" if ds.lifecycle_stage.value == "Raw" else "[red]Missing[/red]")

        table.add_row(
            ds.id,
            ds.name,
            ds.structure_type.value,
            ds.lifecycle_stage.value,
            byte_str,
            rec_str,
            col_str,
            ver_str,
            val_str,
        )

    console.print(table)


def display_data_objects_table(repo: MetadataRepository):
    table = Table(
        title="Physical Data Objects (OMDObjects)",
        box=box.ROUNDED,
        header_style="bold blue",
    )
    table.add_column("Object ID", style="cyan")
    table.add_column("Dataset ID", style="green")
    table.add_column("Format", style="white")
    table.add_column("Size (Bytes)", justify="right")
    table.add_column("Records", justify="right")
    table.add_column("Compression", style="yellow")
    table.add_column("Columns", justify="right")

    for do_id, omd in repo.omd_objects.items():
        table.add_row(
            omd.data_object_id,
            omd.dataset_id,
            omd.physical_type,
            f"{omd.volume.byte_size:,}",
            f"{omd.volume.record_count:,}" if omd.volume.record_count is not None else "-",
            omd.variety.compression_algorithm or "NONE",
            str(omd.variety.column_count),
        )

    console.print(table)


def display_lineage_tree(repo: MetadataRepository, dataset_id: str):
    lineage_svc = LineageAndRecEngine(repo)
    upstream = lineage_svc.get_upstream_lineage(dataset_id)
    impact = lineage_svc.get_downstream_impact(dataset_id)

    ds_name = repo.datasets[dataset_id].name if dataset_id in repo.datasets else dataset_id
    tree = Tree(f"[bold yellow]Lineage Explorer: {ds_name} ({dataset_id})[/bold yellow]")

    # Upstream
    up_branch = tree.add("[bold cyan][^] Upstream Lineage (ProcessedFrom Ancestors)[/bold cyan]")
    if upstream["upstream_datasets"]:
        for up in upstream["upstream_datasets"]:
            up_branch.add(f"[green]{up['name']} ({up['id']})[/green] - [dim]{up['stage']}[/dim]")
    else:
        up_branch.add("[dim]No upstream ancestors (Origin Raw Dataset)[/dim]")

    # Downstream
    down_branch = tree.add(f"[bold red][v] Downstream Blast Radius ({impact['blast_radius_count']} impacted)[/bold red]")
    if impact["impacted_datasets"]:
        for down in impact["impacted_datasets"]:
            down_branch.add(f"[red]{down['name']} ({down['id']})[/red] - [dim]{down['stage']}[/dim]")
    else:
        down_branch.add("[dim]No downstream dependents[/dim]")

    console.print(tree)


def display_governance_audit(repo: MetadataRepository):
    engine = ConstraintEngine(repo)
    alerts = engine.run_all_checks()

    table = Table(
        title="Governance & Constraint Compliance Report (MSM)",
        box=box.ROUNDED,
        header_style="bold red",
    )
    table.add_column("Severity", style="bold", justify="center")
    table.add_column("Rule", style="cyan")
    table.add_column("Dataset ID", style="white")
    table.add_column("Violation Detail", style="yellow")
    table.add_column("Suggested Remediation", style="green")

    if not alerts:
        console.print(Panel("[bold green][PASS] All Governance & TV-Word Constraints Passed![/bold green]", box=box.ROUNDED))
        return

    for alert in alerts:
        sev_color = "red" if alert.severity == ValidationSeverity.CRITICAL else "yellow"
        table.add_row(
            f"[{sev_color}]{alert.severity.value}[/{sev_color}]",
            alert.rule_name,
            alert.dataset_id,
            alert.message,
            alert.suggested_action,
        )

    console.print(table)


def display_freshness_report(repo: MetadataRepository):
    monitor = StalenessAndEvolutionMonitor(repo)
    stale_alerts = monitor.check_staleness()

    if not stale_alerts:
        console.print(Panel("[bold green][PASS] All Datasets are Fresh within expected TTL intervals.[/bold green]", box=box.ROUNDED))
    else:
        table = Table(
            title="Freshness & Staleness Alerts (TTL Monitor)",
            box=box.ROUNDED,
            header_style="bold red",
        )
        table.add_column("Dataset Name", style="bold white")
        table.add_column("TTL (sec)", justify="right")
        table.add_column("Elapsed (sec)", justify="right", style="red")
        table.add_column("Overdue By", justify="right", style="bold red")
        table.add_column("Alert Message", style="yellow")

        for alert in stale_alerts:
            table.add_row(
                alert["dataset_name"],
                str(alert["expected_refresh_interval_sec"]),
                f"{alert['elapsed_sec']}s",
                f"+{alert['overdue_sec']}s",
                alert["alert_message"],
            )
        console.print(table)


def render_full_dashboard(repo: MetadataRepository, target_lineage_id: Optional[str] = None):
    display_catalog_banner()
    console.print()
    display_datasets_table(repo)
    console.print()
    display_data_objects_table(repo)
    console.print()
    if target_lineage_id and target_lineage_id in repo.datasets:
        display_lineage_tree(repo, target_lineage_id)
        console.print()
    display_governance_audit(repo)
    console.print()
    display_freshness_report(repo)
