from common.extensions import PostgresExtensions


def verify(context):
    return PostgresExtensions(context).run()
