from abc import abstractmethod
import os
import time
from celery import Celery, chain
from celery_once import QueueOnce
from abc import ABCMeta, abstractmethod

from project.server.request import Args

celery = Celery(__name__)
celery.conf.broker_url = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379")
celery.conf.result_backend = os.environ.get("CELERY_RESULT_BACKEND", "redis://localhost:6379")
celery.conf.task_serializer = 'pickle'
celery.conf.result_serializer = 'pickle'
celery.conf.accept_content = ['application/json', 'application/x-python-serialize']
celery.conf.ONCE = {
  'backend': 'celery_once.backends.Redis',
  'settings': {
    'url': 'redis://redis:6379/0',
    'default_timeout': 60 * 60
  }
}

class IRepository(metaclass=ABCMeta):
    @abstractmethod
    def get(self) -> int:
        pass

class Repository(IRepository):
    def get(self) -> int:
        return 5

class Sout(celery.Task, QueueOnce):
    repository: IRepository

    def run(self, args: Args):
        print(f"sout {args.name}!")
        time.sleep(self.repository.get())
        return True

class Start(celery.Task, QueueOnce):
    repository: IRepository

    def run(self, args: Args):
        raise NotImplementedError
        # print(f"start {args.name}!")
        # time.sleep(self.repository.get())
        # return True

class Sin(celery.Task, QueueOnce):
    repository: IRepository
 
    def run(self, args: Args):
        print(f"sin {args.name}!")
        time.sleep(self.repository.get())
        return True

class Stop(celery.Task, QueueOnce):
    repository: IRepository
 
    def run(self, args: Args):
        print(f"stop {args.name}!")
        time.sleep(self.repository.get())
        return True     

class DamageControl(celery.Task, QueueOnce):
 
    def run(self, id, name):
        args = Args(id, name)
        workflow = [
          sout.si(args),
          stop.si(args),
          start.si(args),
          sin.si(args)
        ]
        chain(*workflow).delay()
        return True

repository = Repository()
_sout = Sout()
_sout.repository = repository
_start = Start()
_start.repository = repository
_sin = Sin()
_sin.repository = repository
_stop = Stop()
_stop.repository = repository

sout = celery.register_task(_sout)
start = celery.register_task(_start)
sin = celery.register_task(_sin)
stop = celery.register_task(_stop)
damage_control = celery.register_task(DamageControl())