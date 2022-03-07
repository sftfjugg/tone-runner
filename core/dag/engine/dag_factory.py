from abc import ABC, abstractmethod
from typing import NewType


DagId = NewType('DagId', int)


class DagFactory(ABC):

    @abstractmethod
    def object_factory(self):
        """factory method"""

    def generate_dag(self):
        """build dag from job"""
        inst = self.object_factory()
        return inst.build()


class BaseGenerateDag(ABC):

    @abstractmethod
    def build(self):
        """Build the entry to a dag instance, return db dag id"""


def create_dag_from_job(factory: DagFactory) -> DagId:
    return factory.generate_dag()
