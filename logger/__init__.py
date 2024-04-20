import logging


class Logger:
    def __init__(self, file_name, logger_name):
        self.file_name = file_name
        self.logger_name = logger_name

        # Create logger
        self.logger = logging.getLogger(self.logger_name)
        self.logger.setLevel(logging.DEBUG)

        # Create file handler
        fh = logging.FileHandler(self.file_name, mode="w")
        fh.setLevel(logging.DEBUG)

        # Create formatter
        formatter = logging.Formatter("%(asctime)s - %(name)-15s - %(levelname)-8s - %(message)s",
                                      datefmt="%d-%m-%Y %I:%M:%S %p")

        # Add formatter to file handler
        fh.setFormatter(formatter)

        # Add file handler to logger
        self.logger.addHandler(fh)
