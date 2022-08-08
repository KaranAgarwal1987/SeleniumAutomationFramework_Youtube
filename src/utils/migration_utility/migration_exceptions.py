class ConfigException(Exception):
    def __init__(self, exp: Exception):
        self._exp = str(exp)

    def __str__(self):
        return f'Error:{self._exp} parsing configuration.'

    def __repr__(self):
        return str(self)


class LocalFileHandlingException(Exception):
    pass

class WrongReaderException(Exception):
    pass
