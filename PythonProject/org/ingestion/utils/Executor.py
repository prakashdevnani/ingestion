import abc

class Executor(abc.ABC):
    @abc.abstractmethod
    def execute_ingestion(self):
        pass
    @abc.abstractmethod
    def fetch_clients(self):
        pass
    @abc.abstractmethod
    def save_details(self,description,clients,news):
        pass
    @abc.abstractmethod
    def get_news_details(self):
        pass