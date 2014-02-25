
class Function(object):

    @classmethod
    def load_from_file(cls, path):

        script = ''.join(file(path).readlines())
        return cls(script)

    def __init__(self, script):

        self.script = script

