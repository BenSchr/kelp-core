from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from functools import partial
import yaml
from jinja2 import Environment, FileSystemLoader, StrictUndefined, TemplateNotFound
from collections.abc import Iterable


# def load_yaml_with_jinja(
#     path, jinja_context=None, jinja_env=None, stream=False
# ) -> list[dict] | Generator[dict, None, None] | dict:
#     """Load YAML files or directories with Jinja rendering.

#     - If stream=True: returns an iterator yielding parsed dicts for each template.
#     - If path is a directory: returns a dict of {template_name: parsed_dict}.
#     - If path is a file: returns the parsed dict from that file.
#     """
#     iterator = _iter_load_yaml_with_jinja(path, jinja_context=jinja_context, jinja_env=jinja_env)
#     if stream:
#         return iterator

#     p = Path(path)
#     ##
#     items = [item for item in iterator]
#     if p.is_dir():
#         return items
#     # single file -> return the single value or empty dict
#     return items[0] if items else {}


# def _iter_load_yaml_with_jinja(
#     path, jinja_context=None, jinja_env=None
# ) -> Generator[dict, None, None]:
#     """Iterator that yields (name, parsed_dict) for each template in `path`.

#     - If `path` is a directory: yields (template_name, parsed_dict) for each
#       template found by the loader.
#     - If `path` is a file: yields a single (filename, parsed_dict) tuple.
#     """
#     p = Path(path)
#     ctx = jinja_context or {}

#     def _parse_rendered(rendered: str, source_name="<string>") -> dict:
#         try:
#             return yaml.safe_load(rendered) or {}
#         except Exception as e:
#             raise ValueError(f"Failed to parse YAML from rendered template {source_name}: {e}")

#     local_env = jinja_env or Environment(
#         undefined=StrictUndefined,
#         variable_start_string="${",
#         variable_end_string="}",
#     )
#     if p.is_dir():
#         local_env.loader = FileSystemLoader(str(p))
#         for tpl_name in local_env.list_templates():
#             tpl = local_env.get_template(tpl_name)
#             rendered = tpl.render(**ctx)
#             result = {
#                 "file_path": tpl_name,
#                 "rendered_content": _parse_rendered(rendered, source_name=tpl_name),
#             }
#             yield result
#         return

#     # Single file case: render from file text (no filesystem loader needed)
#     text = p.read_text(encoding="utf-8")
#     template = local_env.from_string(text)
#     rendered = template.render(**ctx)
#     result = {
#         "file_path": p.name,
#         "rendered_content": _parse_rendered(rendered, source_name=p.name),
#     }
#     yield result


def render_string_with_jinja(s, jinja_context=None, jinja_env=None):
    """Render a single string as a Jinja template with the provided context."""
    if not isinstance(s, str):
        return s
    # Create a minimal env for simple string rendering; keep StrictUndefined
    local_env = jinja_env or Environment(
        undefined=StrictUndefined, variable_start_string="${", variable_end_string="}"
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
            raise ValueError(f"Failed to parse YAML from rendered template {relative_path}: {e}")
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
