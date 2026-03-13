import argparse
import os
import signal
import smtplib
import sqlite3
import ssl
import sys
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from enum import StrEnum

import psycopg2
from dotenv import load_dotenv
from jinja2 import Template

from log import logger

load_dotenv()

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


def is_postgres_url(db_path: str) -> bool:
    return db_path.startswith("postgresql://") or db_path.startswith("postgres://")


def conn_table(db_path: str):
    """
    Function to create table

    Parameters:
        db_path : Name of sqlite3/ postgres db database
    """
    try:
        if is_postgres_url(db_path):
            conn = None
            conn = psycopg2.connect(db_path)
            cur = conn.cursor()
            cur.execute("""
                    CREATE TABLE IF NOT EXISTS pipeline_state (
                        id SERIAL PRIMARY KEY,
                        last_processed_id INTEGER NOT NULL,
                        status TEXT NOT NULL CHECK(status IN ('PAUSED', 'RUNNING', 'FAILED', 'COMPLETED')),
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        session_notes TEXT
                    );
                """)
            conn.commit()
            logger.info("✅ Tables created or already exist (PostgreSQL)")
            conn.close()
        else:
            connection = None
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
            logger.info("✅ Tables created or aleady exists (SQLite)")
            connection.close()
    except psycopg2.OperationalError as e:
        logger.error(f"❌ Table not created: {e}")
        raise
    except sqlite3.OperationalError as e:
        logger.error(f"❌ Table not created: {e}")


def get_last_processed_id(db_path: str) -> int:
    if is_postgres_url(db_path):
        conn = psycopg2.connect(db_path)
        cur = conn.cursor()
        cur.execute("SELECT last_processed_id FROM pipeline_state ORDER BY id DESC LIMIT 1")
        row = cur.fetchone()
        conn.close()
        return row[0] if row else 0
    else:
        connection = sqlite3.connect(db_path)
        cur = connection.cursor()
        cur.execute("SELECT last_processed_id FROM pipeline_state ORDER BY id DESC LIMIT 1")
        row = cur.fetchone()
        connection.close()
        return row[0] if row else 0


def save_state(last_id: int, status: PipelineStatus, db_path: str, note: str | None = None):
    pass
    if is_postgres_url(db_path):
        conn = psycopg2.connect(db_path)
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO pipeline_state (last_processed_id, status, session_notes) VALUES (%s, %s, %s)",
            (last_id, status.value, note),
        )
        conn.commit()
        logger.info(f"📌 Saved state: status={status.value}, last_id={last_id} (PostgreSQL)")
        conn.close()
    else:
        connection = sqlite3.connect(db_path)
        cur = connection.cursor()
        cur.execute(
            "INSERT INTO pipeline_state (last_processed_id, status, session_notes) VALUES (?, ?, ?)",
            (last_id, status.value, note),
        )
        connection.commit()
        logger.info(f"📌 Saved state: status={status.value}, last_id={last_id} (SQLite)")
        connection.close()


def signal_interrupt(sig, frame):
    global running, interrupted
    interrupted = True
    running = False
    logger.warning(f"⚠️ Interrupt received: {sig}")


signal.signal(signal.SIGINT, signal_interrupt)
if hasattr(signal, "SIGTERM"):
    signal.signal(signal.SIGTERM, signal_interrupt)


def send_email(server: str, sender: str, recipient: object, password: str, last_id: int, status: PipelineStatus):
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(server, 465, context=context) as email:
        email.login(sender, password)

        msg = MIMEMultipart("alternative")
        msg["From"] = sender
        msg["To"] = recipient
        msg["Subject"] = "PIPELINE RUN Alert"

        with open("index.html", encoding="utf-8") as file:
            html_content = Template(file.read())
            rendered_content = html_content.render(last_id=last_id, status=status)
            msg.attach(MIMEText(rendered_content, "html"))

            email.sendmail(sender, recipient, msg.as_string())
            logger.info(" 🚀 Pipeline status email sent")


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

    max_retries = 3
    base_delay = 1

    current_id = get_last_processed_id(db_path) if resume else 0
    target_id = max_records + current_id
    failed = False

    while running and current_id < target_id:
        for attempt in range(1, max_retries + 1):
            try:
                batch_start = current_id + 1
                batch_end = min(current_id + batch_size, target_id)
                logger.info(f"🚀 Processing records {batch_start} to {batch_end} (attempt {attempt})")
                time.sleep(sleep_seconds)

                current_id = batch_end

                break
            except Exception as e:
                logger.error(f"❌ Attempt {attempt} failed: {e}")
                if attempt == max_retries:
                    failed = True
                    running = False
                else:
                    backoff = base_delay * (2 ** (attempt - 1))
                    logger.warning(f"⚠️ Retrying in {backoff}s")
                    time.sleep(backoff)
        else:
            continue

        if interrupted:
            break

        if current_id < target_id and not interrupted:
            save_state(batch_end, PipelineStatus.RUNNING, db_path, note=f"Processed batch {batch_start}-{batch_end}")

    # Final status of run
    if interrupted:
        save_state(current_id, PipelineStatus.PAUSED, db_path, note="Interrupted by signal")
        send_email(
            "smtp.gmail.com",
            os.getenv("SENDER"),
            os.getenv("RECIPIENT"),
            os.getenv("PASSWORD"),
            current_id,
            PipelineStatus.PAUSED,
        )
        logger.info(f" 📌 Run ended at record {current_id} with status=PAUSED")
    elif current_id >= target_id:
        save_state(current_id, PipelineStatus.COMPLETED, db_path, note="Completed normally")
        send_email(
            "smtp.gmail.com",
            os.getenv("SENDER"),
            os.getenv("RECIPIENT"),
            os.getenv("PASSWORD"),
            current_id,
            PipelineStatus.COMPLETED,
        )
        logger.info(f" 📌 Run ended at record {current_id} with status=COMPLETED")
    elif failed:
        send_email(
            "smtp.gmail.com",
            os.getenv("SENDER"),
            os.getenv("RECIPIENT"),
            os.getenv("PASSWORD"),
            current_id,
            PipelineStatus.FAILED,
        )
        sys.exit(1)


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
