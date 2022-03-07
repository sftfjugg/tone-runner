import config


class EnvType:
    name = 'test'  # test or prod
    env = 'dev'  # dev, daily, pre, prod

    @classmethod
    def set_env(cls, env):
        env = str(env).strip()
        EnvType.env = env
        if env in ('pre', 'prod'):
            EnvType.name = 'prod'
        else:
            EnvType.name = 'test'

    @classmethod
    def is_test(cls):
        return EnvType.name == 'test'


EnvType.set_env(config.ENV_TYPE)
