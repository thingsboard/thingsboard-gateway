
class EventStorageReaderPointer:
    def __init__(self, file, line):
        self.file = file
        self.line = line

    def __eq__(self, other):
        return self.file == other.file and self.line == other.line

    def __hash__(self):
        return hash((self.file, self.line))
