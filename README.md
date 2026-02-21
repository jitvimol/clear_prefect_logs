# Prefect Database Maintenance & Cleanup Flow

A robust, automated utility designed to prevent Prefect database bloat by cleaning up old flow runs, task runs, logs, artifacts, and events.

## ⚡ The Problem: Metadata Accumulation
Prefect tracks every state change, log line, and artifact produced by your workflows. If you run high-frequency tasks or long-running pipelines, your database (PostgreSQL/SQLite) will inevitably experience:

* **Performance Degradation:** Slow UI loading times and sluggish API responses.
* **Database Bloat:** "Dead tuples" (deleted rows that haven't been vacuumed) occupy disk space without releasing it to the OS.
* **Storage Costs:** Rapidly increasing disk usage for your RDS or managed database instances.
* **Index Inefficiency:** Massive tables lead to fragmented indexes, making even simple queries slow.

## 🛠️ What This Script Does
This utility provides a self-orchestrating Prefect Flow that performs a "deep clean" of your environment. It combines **API-level cleanup** (to ensure data integrity) with **Direct Database Optimization** (for raw speed).

### Key Features:
1.  **Retention Management:** Automatically identifies and deletes flow runs, task runs, and artifacts older than a configurable threshold (default: 15 days).
2.  **High-Volume Log Purge:** Uses direct SQLAlchemy queries to batch-delete millions of log rows efficiently, bypassing the overhead of the Prefect API.
3.  **Event & Resource Cleanup:** Targets the `events` and `event_resources` tables to remove audit trail bloat often missed by standard cleanup scripts.
4.  **Health Reporting:** * **Size Reporting:** Logs table sizes before and after cleanup.
    * **Bloat Analysis:** Monitors "dead tuples" to identify where physical disk space is being wasted.
5.  **Disk Space Reclamation:** Executes `VACUUM FULL` on core tables. Unlike a standard `DELETE`, `VACUUM FULL` rewrites the table to shrink the physical file size on disk.

## 🗑️ Data Cleanup Summary

The script performs a multi-layered cleanup across the Prefect schema. It distinguishes between **Orchestration Objects** (via API) and **System Metadata** (via Direct SQL) to ensure the database remains lean.

### 1. Orchestration Data (API-Level)
These items are deleted via the Prefect Client to ensure that all associated dependencies and states are handled correctly by the Prefect engine.

| Entity | Description | Logic |
| :--- | :--- | :--- |
| **Flow Runs** | History of executed flows. | Deletes `COMPLETED`, `FAILED`, and `CANCELLED` runs. |
| **Task Runs** | Individual task executions within flows. | Clears all task run entries older than the threshold. |
| **Artifacts** | Result summaries, Markdown reports, and links. | Removes generated artifacts and their metadata. |
| **States** | Flow and Task state history. | **Automatically** cascaded/deleted by the API when runs are removed. |

### 2. System Metadata (Database-Level)
These tables often contain the highest volume of data. The script bypasses the API to delete these in high-speed batches.

| Table | Data Content | Impact of Cleanup |
| :--- | :--- | :--- |
| `log` | All standard out and logger messages. | Significantly reduces DB size; speeds up Log UI loading. |
| `events` | Audit trail of every system occurrence. | Streamlines the "Event Feed" in the Prefect dashboard. |
| `event_resources` | Metadata mapping for events. | Linked to events; usually the largest table in high-scale setups. |

### 3. Physical Storage Optimization
Standard `DELETE` operations in PostgreSQL do not reduce the file size on your hard drive; they only create "holes" for new data.

* **Dead Tuple Removal:** Identifies "ghost" rows that are taking up space.
* **Reclaiming Space:** Uses `VACUUM FULL` to rewrite tables, physically shrinking the database files and returning storage to the Operating System.

## 🚀 Setup & Usage

### 1. Prerequisites
Ensure your environment includes a configuration for the database connection. The script expects:
* `PREFECT_DB_URL_SYNC`: A synchronous SQLAlchemy connection string (e.g., `postgresql://user:pass@localhost:5432/prefect`).
* The `utilities` module for custom naming conventions.

### 2. Deployment
The script includes a deployment block to schedule this cleanup automatically. By default, it is configured for:
* **Schedule:** `0 3 1 * *` (3:00 AM on the 1st of every month).
* **Work Pool:** `process-pool`.
* **Timezone:** `Asia/Bangkok`.

To deploy the flow to your Prefect server:
```bash
python clear_prefect_log.py
