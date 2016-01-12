import logging
from logging import handlers


def logger(name, log_path=None, email_config={}, console=True, rotating=False, rotating_config={}):
    """
    Creates a logger.

    Can also log errors to email if a dict of the following form
    is passed in as `email_config`:

        {
            'host': 'smtp.gmail.com',
            'port': 587,
            'user': 'someone@gmail.com',
            'pass': 'somepass',
            'admins': ['me@me.com']
        }
    """

    # Create the logger.
    logger = logging.getLogger(name)

    # Configure the logger.
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Output to file
    if log_path is not None:
        if rotating:
            # Add a rotating log handler
            rconfig = {
                'maxBytes': 10000,
                'backupCount': 5
            }
            rconfig.update(rotating_config)
            fh = handlers.RotatingFileHandler(log_path, **rconfig)
        else:
            fh = logging.FileHandler(log_path)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    # Output to console.
    if console:
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        logger.addHandler(ch)


    if email_config:
        # Output to email.
        mh = handlers.SMTPHandler(
                (email_config['host'], email_config['port']),
                email_config['user'],
                email_config['admins'],
                '{} Error :('.format(name),
                credentials=(
                    email_config['user'],
                    email_config['pass']
                ),
                secure=()
        )
        mh.setLevel(logging.ERROR)
        logger.addHandler(mh)

    return logger
