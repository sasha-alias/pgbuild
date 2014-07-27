
class Function(object):

    @classmethod
    def load_from_file(cls, path):

        script = file(path).read()+'\n'
        return cls(script)

    def __init__(self, script):

        self.script = script

