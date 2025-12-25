import sys
from typing import Optional
from networksecurity.logging import logger

class NetworkSecurityException(Exception):
    """
    Custom exception class for Network Security project
    """

    def __init__(self, error_message: str, error_details: sys):
        """
        :param error_message: Custom error message
        :param error_details: sys module to extract exception info
        """
        self.error_message = error_message
        _, _, exc_tb = error_details.exc_info()

        self.line_number = exc_tb.tb_lineno if exc_tb else None
        self.file_name = exc_tb.tb_frame.f_code.co_filename if exc_tb else None

        super().__init__(self.error_message)

    def __str__(self):
        if self.line_number and self.file_name:
            return (
                f"Error occurred in file [{self.file_name}] "
                f"at line [{self.line_number}] : {self.error_message}"
            )
        return self.error_message
