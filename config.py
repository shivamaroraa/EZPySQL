import os

config = {
    'sqlite': {
        'database': os.environ.get('SQLITE_DATABASE', '<default_file_name>.db'),
    },
    'production_db': {
        'host': os.environ.get('DB_HOST', '<default_host>'),
        'user': os.environ.get('DB_USER', '<default_user>'),
        'password': os.environ.get('DB_PASSWORD', '<default_password>'),
        'database': os.environ.get('DB_NAME', '<default_database>'),
        'ssl_ca': os.environ.get('SSL_CA', '<default_path_to_ssl_ca>')
    },
}
