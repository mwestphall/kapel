# Configuration module for KAPEL

from environs import Env

# Read config settings from environment variables (and a named env file in CWD if specified),
# do input validation, and return a config object. Note, if a '.env' file exists in CWD it will be used by default.
class GratiaOutputConfig:
    def __init__(self, envFile=None):
        env = Env()
        # Read a .env file if one is specified, otherwise only environment variables will be used.
        env.read_env(envFile, recurse=False, verbose=True)

        # Where to write the APEL message output.
        self.output_path = env.path("OUTPUT_PATH", "/srv/kapel")

        # infrastructure info
        self.infrastructure_type = env.str("INFRASTRUCTURE_TYPE", "grid")
        self.infrastructure_description = env.str("INFRASTRUCTURE_DESCRIPTION", "APEL-KUBERNETES")

        # optionally define number of nodes and processors. Should not be necessary to
        # set a default of 0 here but see https://github.com/apel/apel/issues/241
        self.nodecount = env.int("NODECOUNT", 0)
        self.processors = env.int("PROCESSORS", 0)


        # Gratia config
        self.gratia_config_path = env.str("GRATIA_CONFIG_PATH", None)

        self.gratia_reporter = env.str("GRATIA_REPORTER", None)
        self.gratia_service = env.str("GRATIA_SERVICE", None)
        self.gratia_probe_manager = env.str("GRATIA_PROBE_MANAGER", None)
        self.gratia_probe_version = env.str("GRATIA_PROBE_VERSION", None)
