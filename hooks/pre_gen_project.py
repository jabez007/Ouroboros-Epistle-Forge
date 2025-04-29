# Called before project is generated

project_slug = "{{ cookiecutter.project_slug }}"
if not project_slug.isidentifier():
    raise ValueError(
        f"Invalid project slug: {project_slug}. Must be a valid Python identifier."
    )
