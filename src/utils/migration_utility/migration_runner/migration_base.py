from abc import ABC, abstractmethod

class MigrationBase(ABC):
    def __init__(self, config_objects: list):
        self._config_objects = config_objects

    @abstractmethod
    def run_migration(self):
        pass
