class ErrorHandler:
    def __init__(self, e):
        self.e = e

    def is_char_not_found(self):
        try:
            if "could not be found!" in self.e.args[0]:
                return True
        except IndexError:
            return False

        return False

    def is_operation_not_supported(self):
        try:
            if 'not permitted' in self.e.args[1] or 'not supported' in self.e.args[1]:
                return True
        except IndexError:
            return False

        return False
