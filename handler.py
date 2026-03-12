import signal
import sqlite3
import time
from enum import StrEnum

from log import logger

DB_PATH = "shutdown.db"
running = True
interrupted = False


class PipelineStatus(StrEnum):
    PAUSED = "PAUSED"  # Interrupted
    RUNNING = "RUNNING"  # Currently processing
    COMPLETED = "COMPLETED"  # Normal finish


def conn_table():
    connection = None
    connection = sqlite3.connect(DB_PATH)
    cur = connection.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_state (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                last_processed_id INTEGER,
                status TEXT CHECK(status IN ('PAUSED', 'RUNNING', 'FAILED', 'COMPLETED')),
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                session_notes TEXT
            );
        """)
        connection.commit()
        logger.info(" ✅ Tables created successfully")
    except sqlite3.Error:
        logger.exception("❌ Table creation failed")
        raise
    finally:
        if connection is not None:
            connection.close()


def get_last_processed_id() -> int:
    connection = None
    try:
        connection = sqlite3.connect(DB_PATH)
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


def save_state(last_id: int, status: PipelineStatus, note: str | None = None):
    connection = None
    try:
        connection = sqlite3.connect(DB_PATH)
        cur = connection.cursor()
        cur.execute(
            "INSERT INTO pipeline_state (last_processed_id, status, session_notes) VALUES (?, ?, ?)",
            (last_id, status.value, note),
        )
        connection.commit()
        logger.info(f"📌 Saved state status = {status.value}, last_id={last_id}")
    except sqlite3.Error:
        logger.exception("❌ Failed to save state")
    finally:
        if connection is not None:
            connection.close()


def signal_interrupt(sig, frame):
    global running, interrupted
    interrupted = True
    running = False
    logger.warning("⚠️ OS interrupt received, stopping gracefully")


signal.signal(signal.SIGINT, signal_interrupt)


def state_handoff():
    global running
    processed = get_last_processed_id()
    current_id = processed

    logger.info(f"===== Starting simulation from record {current_id} =====")
    save_state(current_id, PipelineStatus.RUNNING, "🚀 Run started")

    try:
        while running:
            current_id += 1
            logger.info(f"🚀 Fetching batch data for record {current_id}")
            time.sleep(1.5)

            if current_id >= processed + 20:
                running = False
    except Exception:
        logger.exception("❌ Pipeline failed")
    finally:
        if interrupted:
            final_status = PipelineStatus.PAUSED
            note = "Interrupted by signal"
        else:
            final_status = PipelineStatus.COMPLETED
            note = "Completed normally no errors "

        save_state(current_id, final_status, note)
        logger.info(
            f"📌 Simulation ended at record {current_id} with status = {final_status.value}"
        )


def main():
    state_handoff()


if __name__ == "__main__":
    main()
