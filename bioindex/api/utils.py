from ..lib import aws


def connect_to_bio(config):
    """
    Connect to the index schema.
    """
    return aws.connect_to_db(**config.rds_config, schema=config.bio_schema)


def connect_to_portal(config):
    """
    The portal/metadata schema is completely optional.
    """
    if config.portal_schema:
        return aws.connect_to_db(**config.portal_rds_config, schema=config.portal_schema)
