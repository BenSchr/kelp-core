from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from pathlib import Path

import yaml
from jinja2 import Environment, FileSystemLoader, StrictUndefined, TemplateNotFound


def render_string_with_jinja(s, jinja_context=None, jinja_env=None):
    """Render a single string as a Jinja template with the provided context."""
    if not isinstance(s, str):
        return s
    # Create a minimal env for simple string rendering; keep StrictUndefined
    local_env = jinja_env or Environment(
        undefined=StrictUndefined,
        variable_start_string="${",
        variable_end_string="}",
        autoescape=True,
    )

    try:
        template = local_env.from_string(s)
        return template.render(**(jinja_context or {}))
    except Exception as e:
        # Surface rendering errors instead of silently returning the input
        raise ValueError(f"Failed to render string as Jinja template: {e}") from e


def render_obj_with_jinja(obj, jinja_context=None, jinja_env=None):
    if isinstance(obj, str):
        return render_string_with_jinja(obj, jinja_context, jinja_env)
    if isinstance(obj, dict):
        return {k: render_obj_with_jinja(v, jinja_context, jinja_env) for k, v in obj.items()}
    if isinstance(obj, list):
        return [render_obj_with_jinja(v, jinja_context, jinja_env) for v in obj]
    return obj


def _render_jinja(
    rel_path: str,
    context: dict,
    env: Environment,
) -> str:
    """Render a Jinja template by relative path."""
    try:
        template = env.get_template(rel_path)
    except TemplateNotFound as e:
        raise FileNotFoundError(f"Template not found: {rel_path}") from e
    return template.render(**context)


def load_yaml_with_jinja(
    path: str | Path,
    jinja_context: dict | None = None,
    jinja_env: Environment | None = None,
    patterns: Iterable[str] = ("*.yml", "*.yaml"),
    recursive: bool = True,
) -> dict:
    ## Load YAML File with Jinja
    path = Path(path)
    context = jinja_context or {}

    # Determine base directory for Jinja search path
    base_dir = path if path.is_dir() else path.parent
    loader = FileSystemLoader(str(base_dir))
    env = jinja_env or Environment(
        loader=loader,
        undefined=StrictUndefined,
        variable_start_string="${",
        variable_end_string="}",
        autoescape=True,
    )
    files: list[Path]
    if path.is_file():
        files = [path]
    elif path.is_dir():
        files = []
        for pattern in patterns:
            if recursive:
                files.extend(base_dir.rglob(pattern))
            else:
                files.extend(base_dir.glob(pattern))
        seen = set()
        files = [f for f in files if not (f in seen or seen.add(f))]
    else:
        raise ValueError(f"Path {path} is neither a file nor a directory.")

    def load_and_render(file_path: Path, base_dir: Path) -> dict:

        relative_path = file_path.relative_to(base_dir)

        rendered_content = _render_jinja(str(relative_path), context, env)
        try:
            parsed_yaml = yaml.safe_load(rendered_content) or {}
        except Exception as e:
            raise ValueError(
                f"Failed to parse YAML from rendered template {relative_path}: {e}",
            ) from e
        return _attach_file_path(parsed_yaml, relative_path)

    result: dict = {}

    # Use executor.map with a partial to keep the code concise and still run
    # load_and_render concurrently for each file.
    loader_func = partial(load_and_render, base_dir=base_dir)
    with ThreadPoolExecutor() as executor:
        for parsed in executor.map(loader_func, files):
            result = _deep_merge_dicts(result, parsed)

    # for file_path in files:
    #     # relative_path = file_path.relative_to(base_dir)

    #     # rendered_content = _render_jinja(str(relative_path), context, env)
    #     # try:
    #     #     parsed_yaml = yaml.safe_load(rendered_content) or {}
    #     # except Exception as e:
    #     #     raise ValueError(f"Failed to parse YAML from rendered template {relative_path}: {e}")
    #     # parsed_yaml = _attach_file_path(parsed_yaml, relative_path)

    return result


def _attach_file_path(
    parsed_yaml: dict,
    file_path: Path,
) -> dict:
    """Attach file path metadata to the parsed YAML dict."""
    if parsed_yaml.get("kelp_models"):
        for model in parsed_yaml["kelp_models"]:
            model["origin_file_path"] = str(file_path)
    if parsed_yaml.get("kelp_metric_views"):
        for metric_view in parsed_yaml["kelp_metric_views"]:
            metric_view["origin_file_path"] = str(file_path)
    if parsed_yaml.get("kelp_functions"):
        for function in parsed_yaml["kelp_functions"]:
            function["origin_file_path"] = str(file_path)
    if parsed_yaml.get("kelp_abacs"):
        for abac in parsed_yaml["kelp_abacs"]:
            abac["origin_file_path"] = str(file_path)
    if parsed_yaml.get("kelp_sources"):
        for source in parsed_yaml["kelp_sources"]:
            source["origin_file_path"] = str(file_path)
    return parsed_yaml


def _deep_merge_dicts(result: dict, parsed_yaml: dict) -> dict:
    for key, value in parsed_yaml.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge_dicts(result[key], value)
        elif key in result and isinstance(result[key], list) and isinstance(value, list):
            result[key].extend(value)
        else:
            result[key] = value
    return result
