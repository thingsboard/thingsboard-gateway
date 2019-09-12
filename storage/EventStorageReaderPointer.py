
class EventStorageReaderPointer:
    def __init__(self):
        self.file = None
        self.line = None

    def __eq__(self, other):
        return self.file == other.file and self.line == other.line

    def __hash__(self):
        return hash((self.file, self.line))

    def get_file(self):
        return self.file

    def get_line(self):
        return self.line

    def set_file(self, file):
        self.file = file

    def set_line(self, line):
        self.line = line





