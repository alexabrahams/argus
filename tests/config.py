import functools
import os
import socket

import pytest

from tests.util import get_random_id

SESSION_ID_LEN = 8


class Config:
    __slots__ = ()

    def __init__(self, **kwargs):
        [setattr(self, k, v) for (k, v) in kwargs.items()]

    def update(self, cfg):
        for k in cfg:
            if k not in self.__slots__:
                raise ValueError(f"Unknown config option: {k}")
            setattr(self, k, cfg[k])


def requires_config(cfg, vars_):
    """Decorator for fixtures that will skip tests if the required config variables
    are missing or undefined in the configuration
    """

    def decorator(f):
        # We need to specify 'request' in the args here to satisfy pytest's fixture logic
        @functools.wraps(f)
        def wrapper(request, *args, **kwargs):
            for var in vars_:
                if not getattr(cfg, var):
                    pytest.skip(f"config variable {var} missing, skipping test")
            return f(request, *args, **kwargs)

        return wrapper

    return decorator


def yield_requires_config(cfg, vars_):
    """As above but for py.test yield_fixtures"""

    def decorator(f):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            for var in vars_:
                if not getattr(cfg, var):
                    pytest.skip(f"config variable {var} missing, skipping test")
            gen = f(*args, **kwargs)
            yield next(gen)

        return wrapper

    return decorator


class FixtureConfig(Config):
    __slots__ = (
        "java_executable",
        "jenkins_url",
        "jenkins_war",
        "jenkins_image",
        "minio_executable",
        "minio_image",
        "mongo_bin",
        "mongo_image",
        "pg_config_executable",
        "redis_executable",
        "redis_image",
        "rethink_executable",
        "rethink_image",
        "httpd_executable",
        "httpd_image",
        "httpd_modules",
        "fixture_hostname",
        "xvfb_executable",
        "disable_proxy",
        "server_class",
        "session_id",
        "k8s_namespace",
        "k8s_local_test",
    )


# Default values for system resource locations - patch this to change defaults
try:
    DEFAULT_SERVER_FIXTURES_HOSTNAME = socket.gethostbyname(socket.gethostname())
except socket.gaierror:
    DEFAULT_SERVER_FIXTURES_HOSTNAME = "127.0.0.1"
DEFAULT_SERVER_FIXTURES_SESSION_ID = get_random_id(SESSION_ID_LEN)
DEFAULT_SERVER_FIXTURES_DISABLE_HTTP_PROXY = True
DEFAULT_SERVER_FIXTURES_SERVER_CLASS = "thread"
DEFAULT_SERVER_FIXTURES_K8S_NAMESPACE = None
DEFAULT_SERVER_FIXTURES_K8S_LOCAL_TEST = False
DEFAULT_SERVER_FIXTURES_JAVA = "java"
DEFAULT_SERVER_FIXTURES_JENKINS_URL = "http://acmejenkins.example.com"
DEFAULT_SERVER_FIXTURES_JENKINS_WAR = "/usr/share/jenkins/jenkins.war"
DEFAULT_SERVER_FIXTURES_JENKINS_IMAGE = "jenkins/jenkins:2.138.3-alpine"
DEFAULT_SERVER_FIXTURES_MINIO = "minio"
DEFAULT_SERVER_FIXTURES_MINIO_IMAGE = "minio/minio:latest"
DEFAULT_SERVER_FIXTURES_MONGO_BIN = "mongod"
DEFAULT_SERVER_FIXTURES_MONGO_IMAGE = "mongo:3.6"
DEFAULT_SERVER_FIXTURES_PG_CONFIG = "pg_config"
DEFAULT_SERVER_FIXTURES_REDIS = "redis-server"
DEFAULT_SERVER_FIXTURES_REDIS_IMAGE = "redis:5.0.2-alpine"
DEFAULT_SERVER_FIXTURES_RETHINK = "rethinkdb"
DEFAULT_SERVER_FIXTURES_RETHINK_IMAGE = "rethinkdb:2.3.6"
DEFAULT_SERVER_FIXTURES_HTTPD = "apache2"
DEFAULT_SERVER_FIXTURES_HTTPD_IMAGE = "httpd:2.4.37"
DEFAULT_SERVER_FIXTURES_HTTPD_MODULES = "/usr/lib/apache2/modules"
DEFAULT_SERVER_FIXTURES_XVFB = "Xvfb"

CONFIG = FixtureConfig(
    # Not using localhost here in case we are being used in a cluster-type job
    fixture_hostname=os.getenv("SERVER_FIXTURES_HOSTNAME", DEFAULT_SERVER_FIXTURES_HOSTNAME),
    disable_proxy=os.getenv("SERVER_FIXTURES_DISABLE_HTTP_PROXY", DEFAULT_SERVER_FIXTURES_DISABLE_HTTP_PROXY),
    server_class=os.getenv("SERVER_FIXTURES_SERVER_CLASS", DEFAULT_SERVER_FIXTURES_SERVER_CLASS),
    k8s_namespace=os.getenv("SERVER_FIXTURES_K8S_NAMESPACE", DEFAULT_SERVER_FIXTURES_K8S_NAMESPACE),
    k8s_local_test=os.getenv("SERVER_FIXTURES_K8S_LOCAL_TEST", DEFAULT_SERVER_FIXTURES_K8S_LOCAL_TEST),
    session_id=os.getenv("SERVER_FIXTURES_SESSION_ID", DEFAULT_SERVER_FIXTURES_SESSION_ID),
    java_executable=os.getenv("SERVER_FIXTURES_JAVA", DEFAULT_SERVER_FIXTURES_JAVA),
    jenkins_war=os.getenv("SERVER_FIXTURES_JENKINS_WAR", DEFAULT_SERVER_FIXTURES_JENKINS_WAR),
    jenkins_image=os.getenv("SERVER_FIXTURES_JENKINS_IMAGE", DEFAULT_SERVER_FIXTURES_JENKINS_IMAGE),
    minio_executable=os.getenv("SERVER_FIXTURES_MINIO", DEFAULT_SERVER_FIXTURES_MINIO),
    minio_image=os.getenv("SERVER_FIXTURES_MINIO_IMAGE", DEFAULT_SERVER_FIXTURES_MINIO_IMAGE),
    mongo_bin=os.getenv("SERVER_FIXTURES_MONGO_BIN", DEFAULT_SERVER_FIXTURES_MONGO_BIN),
    mongo_image=os.getenv("SERVER_FIXTURES_MONGO_IMAGE", DEFAULT_SERVER_FIXTURES_MONGO_IMAGE),
    pg_config_executable=os.getenv("SERVER_FIXTURES_PG_CONFIG", DEFAULT_SERVER_FIXTURES_PG_CONFIG),
    redis_executable=os.getenv("SERVER_FIXTURES_REDIS", DEFAULT_SERVER_FIXTURES_REDIS),
    redis_image=os.getenv("SERVER_FIXTURES_REDIS_IMAGE", DEFAULT_SERVER_FIXTURES_REDIS_IMAGE),
    rethink_executable=os.getenv("SERVER_FIXTURES_RETHINK", DEFAULT_SERVER_FIXTURES_RETHINK),
    rethink_image=os.getenv("SERVER_FIXTURES_RETHINK_IMAGE", DEFAULT_SERVER_FIXTURES_RETHINK_IMAGE),
    httpd_executable=os.getenv("SERVER_FIXTURES_HTTPD", DEFAULT_SERVER_FIXTURES_HTTPD),
    httpd_modules=os.getenv("SERVER_FIXTURES_HTTPD_MODULES", DEFAULT_SERVER_FIXTURES_HTTPD_MODULES),
    httpd_image=os.getenv("SERVER_FIXTURES_HTTPD_IMAGE", DEFAULT_SERVER_FIXTURES_HTTPD_IMAGE),
    xvfb_executable=os.getenv("SERVER_FIXTURES_XVFB", DEFAULT_SERVER_FIXTURES_XVFB),
)
