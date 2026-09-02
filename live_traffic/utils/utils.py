import logging

from datetime import datetime

import pytz


# -----------------------------
# Define timezone
# -----------------------------

ist = pytz.timezone(
    "Asia/Kolkata"
)


# -----------------------------
# IST logging formatter
# -----------------------------

class ISTFormatter(
    logging.Formatter
):

    def formatTime(
        self,
        record,
        datefmt=None
    ):

        dt = datetime.fromtimestamp(
            record.created,
            ist
        )

        return dt.strftime(
            datefmt
        )


# -----------------------------
# Configure logging
# -----------------------------

def configure_logging():

    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    date_format = '%d-%m-%Y %H:%M:%S'

    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        datefmt=date_format
    )

    for handler in logging.getLogger().handlers:

        handler.setFormatter(
            ISTFormatter(
                log_format,
                date_format
            )
        )