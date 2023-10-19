from streaming_pipeline import initialize
from streaming_pipeline.flow import build as flow_builder


def build_flow(
    env_file_path: str = ".env",
    logging_config_path: str = "logging.yaml",
    model_cache_dir: str = None,
    debug: bool = False,
):
    initialize(logging_config_path=logging_config_path, env_file_path=env_file_path)

    return flow_builder(model_cache_dir=model_cache_dir, debug=debug)
