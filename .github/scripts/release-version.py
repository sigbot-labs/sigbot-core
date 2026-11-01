#!/usr/bin/env python3
"""Derive an idempotent SemVer release from the merged commit and existing tags."""
import json
import os
import re
import subprocess
import tomllib
from pathlib import Path


class ReleaseVersion:
    @staticmethod
    def determine(title, base, tags, at_head):
        versions = [(tuple(map(int, tag[1:].split('.'))), tag) for tag in tags if re.fullmatch(r'v\d+\.\d+\.\d+', tag)]
        current = [tag for tag in at_head if re.fullmatch(r'v\d+\.\d+\.\d+', tag)]
        if current:
            version = max(current, key=lambda tag: tuple(map(int, tag[1:].split('.'))))[1:]
        elif not versions:
            version = base
        else:
            major, minor, patch = max(versions)[0]
            if re.match(r'^[\w-]+(?:\([^)]*\))?!:', title):
                major, minor, patch = major + 1, 0, 0
            elif re.match(r'^feat(?:\([^)]*\))?:', title):
                minor, patch = minor + 1, 0
            else:
                patch += 1
            version = f'{major}.{minor}.{patch}'
        return {'version': version, 'tag': 'v' + version}

    @classmethod
    def main(cls):
        tags = subprocess.check_output(['git', 'tag', '--list'], text=True).splitlines()
        head = subprocess.check_output(['git', 'tag', '--points-at', 'HEAD'], text=True).splitlines()
        base = tomllib.loads(Path('Cargo.toml').read_text())['workspace']['package']['version']
        result = cls.determine(os.environ.get('PR_TITLE', ''), base, tags, head)
        result['epoch'] = subprocess.check_output(['git', 'show', '-s', '--format=%ct', 'HEAD'], text=True).strip()
        destination = os.environ.get('GITHUB_OUTPUT')
        if destination:
            with Path(destination).open('a') as stream:
                stream.write(''.join(f'{key}={value}\n' for key, value in result.items()))
        print(json.dumps(result))


if __name__ == '__main__':
    ReleaseVersion.main()
