from __future__ import annotations

import shutil
from pathlib import Path

from setuptools import setup
from setuptools.command.build_py import build_py


class BuildPyWithData(build_py):
    def run(self) -> None:
        super().run()
        source = Path(__file__).parent / "data"
        target = Path(self.build_lib) / "fruitloops" / "data"
        if target.exists():
            shutil.rmtree(target)
        shutil.copytree(source, target, ignore=shutil.ignore_patterns(".DS_Store"))


setup(cmdclass={"build_py": BuildPyWithData})
