from celery import Celery

from app.core.config import get_settings


def create_celery() -> Celery:
    """Instantiate and configure the Celery application.

    Key reliability settings:
    - acks_late=True: the broker only removes a message after the task finishes
      (not when it is picked up). If the worker crashes mid-task, the message
      is re-queued and retried by another worker.
    - reject_on_worker_lost=True: complements acks_late — if the worker process
      is killed unexpectedly, the task is rejected back to the queue rather than
      silently lost.
    - result_extended=True: stores task metadata (args, kwargs, worker name) in
      the result backend, which aids debugging.
    """
    settings = get_settings()

    app = Celery(
        "dataprocessing",
        broker=settings.redis_url,
        backend=settings.redis_url,
        include=["app.tasks.process_dataset"],
    )

    app.conf.update(
        # Serialization
        task_serializer="json",
        result_serializer="json",
        accept_content=["json"],
        # Timezone
        timezone="UTC",
        enable_utc=True,
        # Reliability
        task_acks_late=True,
        task_reject_on_worker_lost=True,
        # Result TTL: keep results in Redis for 1 hour
        result_expires=3600,
        result_extended=True,
        # Worker
        worker_prefetch_multiplier=1,  # fetch one task at a time per worker slot
    )

    return app


celery_app = create_celery()
