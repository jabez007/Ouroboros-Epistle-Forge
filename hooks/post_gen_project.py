# Called after project is generated

import os

REMOVE_PATHS = [
    '{% if cookiecutter.use_poetry == "yes" %} requirements.txt {% endif %}',
    '{% if cookiecutter.use_poetry == "no" %} pyproject.toml {% endif %}',
]

for path in REMOVE_PATHS:
    path = path.strip()
    if path and os.path.exists(path):
        if os.path.isdir(path):
            os.rmdir(path)
        else:
            os.unlink(path)

print(
    "\n🎉 Project generated successfully! Run 'poetry install' or 'pip install -r requirements.txt' to get started."
)
