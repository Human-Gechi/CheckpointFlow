import argparse
import signal
import sqlite3
import time
from enum import StrEnum

from log import logger

running = True
interrupted = False


class PipelineStatus(StrEnum):
    PAUSED = "PAUSED"  # Interrupted
    RUNNING = "RUNNING"  # Currently processing
    FAILED = "FAILED"  # Server failure / Wrong data type
    COMPLETED = "COMPLETED"  # Normal finish


def parse_args():
    parser = argparse.ArgumentParser(description="CheckpointFlow pipeline runner")
    parser.add_argument("--db-path", default="shutdown.db", help="SQLite database path")
    parser.add_argument("--batch-size", type=int, default=5, help="Records per processing batch")
    parser.add_argument("--sleep", type=float, default=1.5, help="Sleep seconds per batch")
    parser.add_argument(
        "--max-records",
        type=int,
        default=20,
        help="Maximum records to process in this run",
    )

    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--resume",
        action="store_true",
        help="Resume from last saved checkpoint",
    )
    mode_group.add_argument("--fresh", action="store_true", help="Start from record 0")

    return parser.parse_args()


def conn_table(db_path: str):
    """
    Function to create table

    Parameters:
        db_path : Name of sqlite3 database
    """
    connection = None
    try:
        connection = sqlite3.connect(db_path)
        cur = connection.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_state (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                last_processed_id INTEGER NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('PAUSED', 'RUNNING', 'FAILED', 'COMPLETED')),
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                session_notes TEXT
            );
        """)
        connection.commit()
        logger.info("✅ Tables created or aleady exists ")
    except sqlite3.Error:
        logger.exception("❌ Table creation failed")
        raise
    finally:
        if connection is not None:
            connection.close()


def get_last_processed_id(db_path: str) -> int:
    connection = None
    try:
        connection = sqlite3.connect(db_path)
        cur = connection.cursor()
        cur.execute("SELECT last_processed_id FROM pipeline_state ORDER BY id DESC LIMIT 1")
        row = cur.fetchone()
        return row[0] if row else 0
    except sqlite3.Error:
        logger.exception("❌ Failed to fetch last_processed_id")
        return 0
    finally:
        if connection is not None:
            connection.close()


def save_state(last_id: int, status: PipelineStatus, db_path: str, note: str | None = None):
    connection = None
    try:
        connection = sqlite3.connect(db_path)
        cur = connection.cursor()
        cur.execute(
            "INSERT INTO pipeline_state (last_processed_id, status, session_notes) VALUES (?, ?, ?)",
            (last_id, status.value, note),
        )
        connection.commit()
        logger.info(f"📌 Saved state: status={status.value}, last_id={last_id}")
    except sqlite3.Error:
        logger.exception("❌Failed to save state")
    finally:
        if connection is not None:
            connection.close()


def signal_interrupt(sig, frame):
    global running, interrupted
    interrupted = True
    running = False
    logger.warning(f"⚠️ Interrupt received: {sig}")


signal.signal(signal.SIGINT, signal_interrupt)
if hasattr(signal, "SIGTERM"):
    signal.signal(signal.SIGTERM, signal_interrupt)


def state_handoff(
    max_records: int,
    db_path: str,
    batch_size: int,
    sleep_seconds: float,
    resume: bool,
):
    global running, interrupted

    running = True
    interrupted = False
    failed = False

    start_id = get_last_processed_id(db_path) if resume else 0
    current_id = start_id
    target_id = start_id + max_records

    logger.info(f"📌 Starting from record {start_id}")

    max_retries = 3
    base_delay = 1

    while running and current_id < target_id:
        for attempt in range(1, max_retries + 1):
            try:
                batch_start = current_id + 1
                batch_end = min(current_id + batch_size, target_id)
                logger.info(f" 🚀 Processing records {batch_start} to {batch_end} (attempt {attempt})")
                time.sleep(sleep_seconds)

                current_id = batch_end
                break

            except Exception as e:
                logger.exception(f"❌ Attempt {attempt} failed")
                if attempt == max_retries:
                    logger.error(f"DB insertion failed after {max_retries} attempts: {e}")
                    failed = True
                    running = False
                    break
                else:
                    backoff = base_delay * (2 ** (attempt - 1))
                    logger.warning(f"⚠️ Retrying in {backoff}s")
                    time.sleep(backoff)

        if interrupted or failed:
            break

    if interrupted:
        final_status = PipelineStatus.PAUSED
        note = "Interrupted by signal"
    elif failed:
        final_status = PipelineStatus.FAILED
        note = "Unhandled exception"
    elif current_id >= target_id:
        final_status = PipelineStatus.COMPLETED
        note = "Completed normally"
    else:
        final_status = PipelineStatus.FAILED
        note = "Stopped before target"

    save_state(current_id, final_status, db_path, note)
    logger.info(f" 📌 Run ended at record {current_id} with status={final_status.value}")


def main():
    args = parse_args()

    if args.batch_size <= 0:
        raise ValueError("--batch-size must be greater than 0")
    if args.sleep < 0:
        raise ValueError("--sleep must be 0 or greater")
    if args.max_records <= 0:
        raise ValueError("--max-records must be greater than 0")

    resume = not args.fresh

    conn_table(args.db_path)
    logger.info(
        f" 📌Config: db_path={args.db_path}, batch_size={args.batch_size}, "
        f"sleep={args.sleep}, max_records={args.max_records}, resume={resume}"
    )
    state_handoff(
        max_records=args.max_records,
        db_path=args.db_path,
        batch_size=args.batch_size,
        sleep_seconds=args.sleep,
        resume=resume,
    )


if __name__ == "__main__":
    main()
