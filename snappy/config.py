import yaml

def load(file):
    with open(file) as data:
        return yaml.load(data)
