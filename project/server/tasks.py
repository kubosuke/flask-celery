import os
import time

from celery import Celery
from celery_once import QueueOnce


celery = Celery(__name__)
celery.conf.broker_url = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379")
celery.conf.result_backend = os.environ.get("CELERY_RESULT_BACKEND", "redis://localhost:6379")
celery.conf.ONCE = {
  'backend': 'celery_once.backends.Redis',
  'settings': {
    'url': 'redis://redis:6379/0',
    'default_timeout': 60 * 60
  }
}

# You can also define tasks like this:
# 
# @celery.task(name="create_task")
# def create_task(task_type):
#     time.sleep(int(task_type) * 10)
#     return True

class MyTask(celery.Task, QueueOnce):
    def run(self, task_type):
        time.sleep(5)
        return True

create_task = celery.register_task(MyTask())
