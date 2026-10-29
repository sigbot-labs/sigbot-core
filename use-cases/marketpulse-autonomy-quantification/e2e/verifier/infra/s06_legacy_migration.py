from common.migration import LegacyMigrationProbe


def verify(context):
    return LegacyMigrationProbe(context).run()
