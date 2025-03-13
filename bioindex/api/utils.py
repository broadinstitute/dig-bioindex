import types

from ..lib import aws


def monkey_patch_router(router):
    """
    Change the router code so that routes defined from here on are not
    actually added and are skipped instead.
    """
    def add_null_route(self, *args, **kwargs):
        pass

    router.add_api_route = types.MethodType(add_null_route, router)


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
        return aws.connect_to_db(**config.portal_rds_secret, schema=config.portal_schema)
