import yaml

class Config(object):
    def __init__(self):
        self.current_subpath = []

        with open("conf/settings.yml", 'r') as fstream:
            self.data = yaml.load(fstream)

    def get(self, *splitted_path):
        if len(splitted_path) == 0:
            self.current_subpath = []
            raise AttributeError('Must be called with at last one attr')
        src = self.data
        for part in self.current_subpath:
            src = self.data.get(part)
        active_param = splitted_path[0]
        if len(splitted_path) == 1:
            try:
                value = src.get(active_param)
                if value is None:
                    raise AttributeError
                self.current_subpath = []
                return value
            except AttributeError:
                raise self.ConfigException(self, active_param)
        else:
            try:
                if src.get(active_param) is None:
                    raise AttributeError
            except AttributeError:
                raise self.ConfigException(self, active_param)
        self.current_subpath.append(splitted_path[0])
        splitted_path = splitted_path[1:]
        return self.get(*splitted_path)

    class ConfigException(Exception):
        def __init__(self, config, active_param):
            text = (
                'Unable to find Parameter "{}" in config section {}'
                .format(active_param, config.current_subpath)
            )
            config.current_subpath = []
            super().__init__(text)
