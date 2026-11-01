from common.cache import RedisFixture


def verify(context):
    return {"single": RedisFixture(context, 1).run(), "cluster": RedisFixture(context, 3).run()}
