import importlib.util
from pathlib import Path
import subprocess
import unittest


class ReleaseTests(unittest.TestCase):
    root = Path(__file__).resolve().parents[2]

    @classmethod
    def setUpClass(cls):
        path = cls.root / ".github/scripts/release-version.py"
        spec = importlib.util.spec_from_file_location("release_version", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        cls.version = module.ReleaseVersion

    def test_version_bumps_and_rerun_keeps_the_existing_commit_tag(self):
        tags = ["v1.8.9", "v1.10.0", "dirty-12345678", "not-semver"]
        for title, expected in [("fix: retry", "1.10.1"), ("feat(order): reserve", "1.11.0"),
                                ("feat!: change protocol", "2.0.0")]:
            self.assertEqual(self.version.determine(title, "1.0.0", tags, [])["version"], expected)
        self.assertEqual(self.version.determine("feat: retry", "1.0.0", tags, ["v1.10.0"])["version"], "1.10.0")
        self.assertEqual(self.version.determine("fix: first release", "1.0.0", [], [])["tag"], "v1.0.0")

    def test_pr_comment_rejects_older_runs_and_retains_both_phases(self):
        program = """
            import assert from 'node:assert/strict';
            import { PrStatusReporter as R } from './.github/scripts/upsert-pr-status-comment.mjs';
            const c = {kind:'ci',phase:'completed',run:'20',sha:'abc',url:'https://example.test/run',
                       results:{'build|test':{result:'failure'}}};
            const first = R.merge('', c);
            assert.match(first, /build\\\\\\|test/);
            assert.match(first, /❌ failure/);
            assert.equal(R.merge(first, {...c, run:'19'}), first);
            const release = R.merge(first, {...c, kind:'release',run:'21',version:'1.2.3'});
            assert.match(release, /PR build and test/);
            assert.match(release, /Release/);
            assert.match(release, /Artifact version: `1.2.3`/);
            assert.equal(release.split(R.marker).length, 2);
        """
        result = subprocess.run(["node", "--input-type=module", "-e", program], cwd=self.root,
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
