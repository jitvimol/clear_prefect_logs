import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from prefect import flow, task, get_run_logger
from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import (
    FlowRunFilter, 
    FlowRunFilterState, 
    FlowRunFilterStateType, 
    FlowRunFilterStartTime,
    TaskRunFilter,
    TaskRunFilterStartTime
)
from prefect.client.schemas.objects import StateType
from prefect.exceptions import ObjectNotFound
from prefect.schedules import Cron
from utilities.config_local import *
from utilities.utilities import generate_flow_run_name, generate_task_run_name
import sqlalchemy as sa
from sqlalchemy import create_engine, text


# Configurable retention period
DEFAULT_DAYS_TO_KEEP = 15


@task(log_prints=True, name='delete_old_flow_runs', task_run_name=generate_task_run_name)
async def delete_old_flow_runs(
    days_to_keep: int = DEFAULT_DAYS_TO_KEEP,
    batch_size: int = 200
):
    """Delete completed flow runs older than specified days."""
    logger = get_run_logger()
    
    async with get_client() as client:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days_to_keep)
        logger.info(f"Deleting flow runs older than {cutoff}")
        
        # Create filter for old completed flow runs
        flow_run_filter = FlowRunFilter(
            start_time=FlowRunFilterStartTime(before_=cutoff),
            state=FlowRunFilterState(
                type=FlowRunFilterStateType(
                    any_=[StateType.COMPLETED, StateType.FAILED, StateType.CANCELLED]
                )
            )
        )
        
        # Get flow runs to delete
        flow_runs = await client.read_flow_runs(
            flow_run_filter=flow_run_filter,
            limit=batch_size
        )
        
        deleted_total = 0
        
        while flow_runs:
            batch_deleted = 0
            failed_deletes = []
            
            # Delete each flow run through the API
            for flow_run in flow_runs:
                try:
                    await client.delete_flow_run(flow_run.id)
                    deleted_total += 1
                    batch_deleted += 1
                except ObjectNotFound:
                    # Already deleted - treat as success
                    deleted_total += 1
                    batch_deleted += 1
                except Exception as e:
                    logger.warning(f"Failed to delete flow run {flow_run.id}: {e}")
                    failed_deletes.append(flow_run.id)
                
                # Rate limiting
                if batch_deleted % 10 == 0:
                    await asyncio.sleep(0.1)
            
            logger.info(f"Deleted {batch_deleted}/{len(flow_runs)} flow runs (total: {deleted_total})")
            if failed_deletes:
                logger.warning(f"Failed to delete {len(failed_deletes)} flow runs")
            
            # Get next batch
            flow_runs = await client.read_flow_runs(
                flow_run_filter=flow_run_filter,
                limit=batch_size
            )
            
            # Delay between batches
            await asyncio.sleep(0.2)
        
        logger.info(f"Flow runs cleanup complete. Total deleted: {deleted_total}")
        return deleted_total


@task(log_prints=True, name='delete_old_task_runs', task_run_name=generate_task_run_name)
async def delete_old_task_runs(
    days_to_keep: int = DEFAULT_DAYS_TO_KEEP,
    batch_size: int = 200
):
    """Delete task runs older than specified days."""
    logger = get_run_logger()
    
    async with get_client() as client:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days_to_keep)
        logger.info(f"Deleting task runs older than {cutoff}")
        
        # Create filter for old task runs
        task_run_filter = TaskRunFilter(
            start_time=TaskRunFilterStartTime(before_=cutoff)
        )
        
        # Get task runs to delete
        task_runs = await client.read_task_runs(
            task_run_filter=task_run_filter,
            limit=batch_size
        )
        
        deleted_total = 0
        
        while task_runs:
            batch_deleted = 0
            failed_deletes = []
            
            # Delete each task run through the API
            for task_run in task_runs:
                try:
                    await client.delete_task_run(task_run.id)
                    deleted_total += 1
                    batch_deleted += 1
                except ObjectNotFound:
                    deleted_total += 1
                    batch_deleted += 1
                except Exception as e:
                    logger.warning(f"Failed to delete task run {task_run.id}: {e}")
                    failed_deletes.append(task_run.id)
                
                # Rate limiting
                if batch_deleted % 10 == 0:
                    await asyncio.sleep(0.1)
            
            logger.info(f"Deleted {batch_deleted}/{len(task_runs)} task runs (total: {deleted_total})")
            if failed_deletes:
                logger.warning(f"Failed to delete {len(failed_deletes)} task runs")
            
            # Get next batch
            task_runs = await client.read_task_runs(
                task_run_filter=task_run_filter,
                limit=batch_size
            )
            
            # Delay between batches
            await asyncio.sleep(0.2)
        
        logger.info(f"Task runs cleanup complete. Total deleted: {deleted_total}")
        return deleted_total


@task(log_prints=True, name='delete_old_artifacts', task_run_name=generate_task_run_name)
async def delete_old_artifacts(
    days_to_keep: int = DEFAULT_DAYS_TO_KEEP,
    batch_size: int = 200
):
    """Delete artifacts older than specified days."""
    logger = get_run_logger()
    
    async with get_client() as client:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days_to_keep)
        logger.info(f"Deleting artifacts older than {cutoff}")
        
        # Get all artifacts (API doesn't support date filtering directly)
        # We'll filter by date after fetching
        offset = 0
        deleted_total = 0
        
        while True:
            artifacts = await client.read_artifacts(
                limit=batch_size,
                offset=offset
            )
            
            if not artifacts:
                break
            
            batch_deleted = 0
            failed_deletes = []
            
            for artifact in artifacts:
                # Check if artifact is old enough
                if artifact.created and artifact.created < cutoff:
                    try:
                        await client.delete_artifact(artifact.id)
                        deleted_total += 1
                        batch_deleted += 1
                    except ObjectNotFound:
                        deleted_total += 1
                        batch_deleted += 1
                    except Exception as e:
                        logger.warning(f"Failed to delete artifact {artifact.id}: {e}")
                        failed_deletes.append(artifact.id)
                    
                    # Rate limiting
                    if batch_deleted % 10 == 0:
                        await asyncio.sleep(0.1)
            
            logger.info(f"Processed {len(artifacts)} artifacts, deleted {batch_deleted} (total: {deleted_total})")
            if failed_deletes:
                logger.warning(f"Failed to delete {len(failed_deletes)} artifacts")
            
            # If we deleted all in this batch, there might be more old ones
            # Otherwise, move to next batch
            if batch_deleted < len(artifacts):
                offset += batch_size
            
            # Delay between batches
            await asyncio.sleep(0.2)
        
        logger.info(f"Artifacts cleanup complete. Total deleted: {deleted_total}")
        return deleted_total


@task(log_prints=True, name='delete_old_logs_direct', task_run_name=generate_task_run_name)
def delete_old_logs_direct(days_to_keep: int = DEFAULT_DAYS_TO_KEEP, batch_size: int = 50000):
    """Delete logs directly from database in batches (faster for large volumes)."""
    logger = get_run_logger()
    
    try:
        # Create database connection using Prefect database URL
        engine = create_engine(PREFECT_DB_URL_SYNC)
        
        cutoff = datetime.now(timezone.utc) - timedelta(days=days_to_keep)
        logger.info(f"Deleting logs older than {cutoff} directly from database (batch size: {batch_size:,})")
        
        total_deleted = 0
        batch_num = 0
        
        while True:
            with engine.connect() as conn:
                # Delete in batches to avoid long-running transactions
                result = conn.execute(
                    text("""
                        DELETE FROM log 
                        WHERE id IN (
                            SELECT id FROM log 
                            WHERE timestamp < :cutoff 
                            LIMIT :batch_size
                        )
                    """),
                    {"cutoff": cutoff, "batch_size": batch_size}
                )
                conn.commit()
                deleted_count = result.rowcount
                
            total_deleted += deleted_count
            batch_num += 1
            
            if deleted_count > 0:
                logger.info(f"Batch {batch_num}: Deleted {deleted_count:,} logs (total: {total_deleted:,})")
            
            # If we deleted fewer than batch_size, we're done
            if deleted_count < batch_size:
                break
            
            # Small delay between batches to reduce database load
            import time
            time.sleep(0.5)
            
        logger.info(f"Logs cleanup complete. Total deleted: {total_deleted:,}")
        return total_deleted
        
    except Exception as e:
        logger.error(f"Failed to delete logs directly: {e}")
        return 0


@task(log_prints=True, name='delete_old_events_direct', task_run_name=generate_task_run_name)
def delete_old_events_direct(days_to_keep: int = DEFAULT_DAYS_TO_KEEP, batch_size: int = 10000):
    """Delete events and event_resources directly from database in batches."""
    logger = get_run_logger()
    
    try:
        engine = create_engine(PREFECT_DB_URL_SYNC)
        
        cutoff = datetime.now(timezone.utc) - timedelta(days=days_to_keep)
        logger.info(f"Deleting events older than {cutoff} directly from database (batch size: {batch_size:,})")
        
        total_deleted_resources = 0
        total_deleted_events = 0
        batch_num = 0
        
        # Delete in batches
        while True:
            with engine.connect() as conn:
                # First, delete event_resources for this batch of events
                result1 = conn.execute(
                    text("""
                        DELETE FROM event_resources 
                        WHERE event_id IN (
                            SELECT id FROM events 
                            WHERE occurred < :cutoff 
                            LIMIT :batch_size
                        )
                    """),
                    {"cutoff": cutoff, "batch_size": batch_size}
                )
                conn.commit()
                deleted_resources = result1.rowcount
                
                # Then delete the events themselves
                result2 = conn.execute(
                    text("""
                        DELETE FROM events 
                        WHERE id IN (
                            SELECT id FROM events 
                            WHERE occurred < :cutoff 
                            LIMIT :batch_size
                        )
                    """),
                    {"cutoff": cutoff, "batch_size": batch_size}
                )
                conn.commit()
                deleted_events = result2.rowcount
                
            total_deleted_resources += deleted_resources
            total_deleted_events += deleted_events
            batch_num += 1
            
            if deleted_events > 0 or deleted_resources > 0:
                logger.info(f"Batch {batch_num}: Deleted {deleted_events:,} events and {deleted_resources:,} event_resources (total: {total_deleted_events:,} events, {total_deleted_resources:,} resources)")
            
            # If we deleted fewer than batch_size events, we're done
            if deleted_events < batch_size:
                break
            
            # Small delay between batches
            import time
            time.sleep(0.5)
            
        logger.info(f"Events cleanup complete. Deleted {total_deleted_events:,} events and {total_deleted_resources:,} event_resources")
        return {"events": total_deleted_events, "event_resources": total_deleted_resources}
        
    except Exception as e:
        logger.error(f"Failed to delete events directly: {e}")
        return {"events": 0, "event_resources": 0}


@task(log_prints=True, name='vacuum_database', task_run_name=generate_task_run_name)
def vacuum_database():
    """Run VACUUM FULL to reclaim disk space and update statistics.
    
    VACUUM FULL actually rewrites the table files and shrinks them on disk,
    unlike regular VACUUM which only marks dead tuples as reusable.
    
    Note: VACUUM FULL takes an exclusive lock on each table while processing.
    """
    logger = get_run_logger()
    
    # Tables to vacuum in order of priority (largest/most active first)
    tables_to_vacuum = [
        'log',
        'event_resources',
        'events',
        'task_run_state',
        'task_run',
        'flow_run_state',
        'flow_run',
        'artifact'
    ]
    
    try:
        # Create engine with isolation level for VACUUM
        engine = create_engine(
            PREFECT_DB_URL_SYNC,
            isolation_level="AUTOCOMMIT"
        )
        
        logger.info("Starting VACUUM FULL on Prefect tables to reclaim disk space...")
        logger.info("Note: This may take several minutes and will lock each table during processing.")
        
        with engine.connect() as conn:
            for table in tables_to_vacuum:
                logger.info(f"Running VACUUM FULL on table: {table}...")
                try:
                    conn.execute(text(f"VACUUM FULL public.{table}"))
                    logger.info(f"✓ Completed VACUUM FULL on {table}")
                except Exception as table_error:
                    logger.warning(f"Failed to vacuum table {table}: {table_error}")
                    # Continue with other tables even if one fails
                    continue
            
        logger.info("VACUUM FULL complete - disk space has been reclaimed")
        return True
        
    except Exception as e:
        logger.error(f"Failed to vacuum database: {e}")
        return False


@task(log_prints=True, name='check_dead_tuples', task_run_name=generate_task_run_name)
def check_dead_tuples():
    """Check dead tuples and bloat in Prefect tables.
    
    This helps verify if VACUUM FULL is needed or was effective.
    """
    logger = get_run_logger()
    
    try:
        engine = create_engine(PREFECT_DB_URL_SYNC)
        
        with engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT
                        relname AS table_name,
                        n_live_tup,
                        n_dead_tup,
                        CASE 
                            WHEN (n_live_tup + n_dead_tup) = 0 THEN 0
                            ELSE round(100.0 * n_dead_tup / (n_live_tup + n_dead_tup), 2)
                        END AS dead_pct
                    FROM pg_stat_user_tables
                    WHERE schemaname = 'public'
                    AND relname IN ('log', 'events', 'event_resources', 'task_run_state', 
                                   'task_run', 'artifact', 'flow_run_state', 'flow_run')
                    ORDER BY n_dead_tup DESC
                    LIMIT 20
                """)
            )
            
            rows = result.fetchall()
            
            report = "\n=== Dead Tuples & Bloat Report ===\n"
            report += f"{'Table':<25} {'Live Rows':<15} {'Dead Rows':<15} {'Dead %':<10}\n"
            report += "-" * 65 + "\n"
            
            total_dead = 0
            for row in rows:
                table_name, live_tup, dead_tup, dead_pct = row
                report += f"{table_name:<25} {live_tup:>14,} {dead_tup:>14,} {dead_pct:>9}%\n"
                total_dead += dead_tup or 0
            
            report += "-" * 65 + "\n"
            report += f"Total dead tuples: {total_dead:,}\n"
            
            if total_dead > 1000000:
                report += "\n⚠️  High dead tuple count detected - VACUUM FULL recommended\n"
            elif total_dead > 100000:
                report += "\n⚠️  Moderate dead tuple count - VACUUM FULL may help\n"
            else:
                report += "\n✓ Dead tuple count is low - tables are healthy\n"
            
            logger.info(report)
            return report
            
    except Exception as e:
        logger.error(f"Failed to check dead tuples: {e}")
        return ""


@task(log_prints=True, name='get_table_sizes', task_run_name=generate_task_run_name)
def get_table_sizes():
    """Get current table sizes for reporting."""
    logger = get_run_logger()
    
    try:
        engine = create_engine(PREFECT_DB_URL_SYNC)
        
        with engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT 
                        schemaname,
                        relname AS tablename,
                        pg_size_pretty(pg_total_relation_size(schemaname||'.'||relname)) AS total_size,
                        to_char(n_live_tup, 'FM999,999,999') AS row_count
                    FROM pg_stat_user_tables
                    WHERE schemaname = 'public'
                    AND relname IN ('log', 'event_resources', 'events', 'task_run_state', 
                                   'task_run', 'artifact', 'flow_run_state', 'flow_run')
                    ORDER BY pg_total_relation_size(schemaname||'.'||relname) DESC
                """)
            )
            
            rows = result.fetchall()
            
            report = "\n=== Prefect Database Table Sizes ===\n"
            report += f"{'Table':<25} {'Size':<15} {'Rows':<15}\n"
            report += "-" * 55 + "\n"
            
            for row in rows:
                report += f"{row[1]:<25} {row[2]:<15} {row[3]:<15}\n"
            
            logger.info(report)
            return report
            
    except Exception as e:
        logger.error(f"Failed to get table sizes: {e}")
        return ""


@flow(
    log_prints=True, 
    name='clear_prefect_log',
    flow_run_name=generate_flow_run_name,
    retries=1,
    retry_delay_seconds=300
)
async def clear_prefect_log(days_to_keep: int = DEFAULT_DAYS_TO_KEEP):
    """Clean up old Prefect data from database.
    
    Args:
        days_to_keep: Number of days to retain data (default: 15)
    """
    logger = get_run_logger()
    logger.info(f"Starting Prefect cleanup - retaining last {days_to_keep} days of data")
    
    try:
        # Get initial table sizes and dead tuples
        logger.info("Getting initial table sizes...")
        initial_sizes = get_table_sizes()
        
        logger.info("Checking initial dead tuples...")
        initial_bloat = check_dead_tuples()
        
        # Clean up using Prefect API (this also cleans related states)
        logger.info("Cleaning up flow runs via API...")
        flow_runs_deleted = await delete_old_flow_runs(days_to_keep=days_to_keep, batch_size=200)
        
        logger.info("Cleaning up task runs via API...")
        task_runs_deleted = await delete_old_task_runs(days_to_keep=days_to_keep, batch_size=200)
        
        logger.info("Cleaning up artifacts via API...")
        artifacts_deleted = await delete_old_artifacts(days_to_keep=days_to_keep, batch_size=200)
        
        # Clean up logs and events directly (faster for large volumes)
        logger.info("Cleaning up logs directly from database...")
        logs_deleted = delete_old_logs_direct(days_to_keep=days_to_keep)
        
        logger.info("Cleaning up events directly from database...")
        events_deleted = delete_old_events_direct(days_to_keep=days_to_keep)
        
        # Check dead tuples before vacuum
        logger.info("Checking dead tuples before vacuum...")
        pre_vacuum_bloat = check_dead_tuples()
        
        # Vacuum database to reclaim space (VACUUM FULL)
        logger.info("Running VACUUM FULL to reclaim disk space...")
        vacuum_database()
        
        # Get final table sizes and dead tuples
        logger.info("Getting final table sizes...")
        final_sizes = get_table_sizes()
        
        logger.info("Checking final dead tuples...")
        final_bloat = check_dead_tuples()
        
        # Summary report
        summary = f"""
=== Prefect Cleanup Summary ===
Retention Period: {days_to_keep} days

Deleted Records:
- Flow Runs: {flow_runs_deleted:,}
- Task Runs: {task_runs_deleted:,}
- Artifacts: {artifacts_deleted:,}
- Logs: {logs_deleted:,}
- Events: {events_deleted.get('events', 0):,}
- Event Resources: {events_deleted.get('event_resources', 0):,}

Note: Flow run states and task run states are automatically deleted with their parent runs.

{final_sizes}
        """
        
        logger.info(summary)
        
        logger.info("Prefect cleanup completed successfully")
        
    except Exception as e:
        error_msg = f"❌ Prefect cleanup failed: {str(e)}"
        logger.error(error_msg)
        raise


if __name__ == "__main__":
    flow_name = "clear_prefect_log"
    work_pool_name = "process-pool"
    flow_file_name = Path(__file__).name
    
    clear_prefect_log.from_source(
        source=str(Path(__file__).parent), 
        entrypoint=f"{flow_file_name}:{flow_name}",
    ).deploy(
        name=flow_name,
        work_pool_name=work_pool_name,
        tags=["maintenance", "cleanup"],
        parameters={"days_to_keep": DEFAULT_DAYS_TO_KEEP},
        schedules=[
            Cron(
                "0 3 1 * *",  # run at 3AM on day 1 of every month
                timezone="Asia/Bangkok"
            )
        ]         
    )
