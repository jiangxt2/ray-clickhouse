project = "ray-clickhouse"
author = "jiangxt2"
release = "0.1.0"

extensions = ["myst_parser"]
source_suffix = {".md": "markdown"}
root_doc = "index"
exclude_patterns = ["../build"]
nitpicky = True

myst_enable_extensions = ["colon_fence", "deflist"]

html_theme = "alabaster"
html_title = "ray-clickhouse 0.1.0"
