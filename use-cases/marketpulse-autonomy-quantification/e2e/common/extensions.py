"""Own one disposable dual-extension PostgreSQL fixture; never adopt the main case volume."""
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import secrets
import shlex
import shutil
import subprocess
import tempfile
import time

import yaml

from common.rust import RustIntegration
from common.containers import LocalContainerEngine


class PostgresExtensions:
    project = 'sigbot-marketpulse-extensions'
    container = 'sigbot-marketpulse-extensions-postgres'
    volume = 'sigbot-marketpulse-extensions-pg16-v1'
    labels = {'io.sigbot.case': 'marketpulse-autonomy-quantification', 'io.sigbot.fixture': 'postgres-extensions'}
    tests = ['actual_timescale_and_pgvector_functions_execute',
             'actual_timescale_compute_matches_postgres_for_multiday_windows_and_revisions']

    def __init__(self, context):
        self.context = context
        self.repository = context.root.parents[2]
        self.compose = context.root / 'deploy/postgres-extensions/docker-compose.yaml'
        self.bootstrap = self.repository / 'deploy/helm/sigbot/bootstrap/postgres-extensions.sql'
        config = yaml.safe_load(self.compose.read_text())
        self.image = os.environ['MARKETPULSE_POSTGRES_IMAGE']
        self.digest = self.image.split('@sha256:')[-1]
        if (len(self.digest) != 64 or any(character.isspace() for character in self.image)
                or config['volumes']['extensions-data']['name'] != self.volume):
            raise ValueError('extension fixture must pin its verified image and dedicated volume')
        self.engine = LocalContainerEngine("MARKETPULSE_EXTENSIONS_COMMAND").arguments()
        self.password = secrets.token_urlsafe(32)
        self.url = f'postgresql://postgres:{self.password}@127.0.0.1:15440/sigbot_extensions'
        self.deadline = time.monotonic() + context.timeout_seconds
        self.container_id = None
        self.owned = False
        self.log = context.evidence(f'postgres-extensions-{context.round_number}.log', '')
        self.result = {'status': 'RUNNING', 'image': self.image, 'cleanup_complete': False,
                       'database_boundary': 'independent container, loopback 15440, dedicated disposable volume',
                       'main_case_database_untouched': True}

    def command(self, arguments, *, sql=None, cleanup=False, check=True):
        timeout = 30 if cleanup else self.deadline - time.monotonic()
        if timeout <= 0:
            raise TimeoutError('extension fixture exceeded its deadline')
        environment = {key: value for key, value in os.environ.items()
                       if key not in {'DOCKER_HOST', 'DOCKER_CONTEXT', 'CONTAINER_HOST', 'CONTAINER_CONNECTION'}}
        environment.update(HTTPS_PROXY='http://127.0.0.1:8800', HTTP_PROXY='http://127.0.0.1:8800')
        result = subprocess.run(self.engine + arguments, cwd=self.context.root, input=sql,
                                capture_output=True, text=True, timeout=timeout, env=environment, check=False)
        output = (result.stdout + result.stderr).replace(self.password, '[REDACTED]').replace(self.url, '[REDACTED]')
        with self.log.open('a') as stream:
            stream.write(output)
        if check and result.returncode:
            raise RuntimeError(f'extension fixture command failed; inspect {self.log.name}')
        return result

    def metadata(self, kind, name, *, cleanup=False):
        template = '{{.Id}}|{{json .Config.Labels}}' if kind == 'container' else '{{json .Labels}}'
        result = self.command([kind, 'inspect', '--format', template, name], cleanup=cleanup, check=False)
        if result.returncode:
            if any(text in result.stderr.lower() for text in ('no such', 'no container', 'no volume')):
                return None
            raise RuntimeError('cannot verify fixture resource ownership')
        if kind == 'container':
            identifier, raw_labels = result.stdout.strip().split('|', 1)
            return identifier, json.loads(raw_labels) or {}
        return name, json.loads(result.stdout) or {}

    def owned_resources(self, *, cleanup=False):
        container = self.metadata('container', self.container, cleanup=cleanup)
        volume = self.metadata('volume', self.volume, cleanup=cleanup)
        for resource in (container, volume):
            if resource and any(resource[1].get(key) != value for key, value in self.labels.items()):
                raise RuntimeError('refusing to adopt or remove an unmanaged extension fixture resource')
        return container, volume

    def remove_owned(self):
        container, volume = self.owned_resources(cleanup=True)
        if container:
            if self.container_id and container[0] != self.container_id:
                raise RuntimeError('extension container identity changed during this run')
            self.command(['rm', '--force', container[0]], cleanup=True)
        if volume:
            self.command(['volume', 'rm', self.volume], cleanup=True)
        if any(self.owned_resources(cleanup=True)):
            raise RuntimeError('extension fixture resources remain after cleanup')

    def sql(self, script, *, scalar=False):
        arguments = ['exec', '-i', self.container_id, 'psql', '--no-psqlrc', '--no-password',
                     '--host', '/var/run/postgresql', '--username', 'postgres', '--dbname', 'sigbot_extensions',
                     '--set', 'ON_ERROR_STOP=on', '--quiet']
        if scalar:
            arguments += ['--tuples-only', '--no-align']
        return self.command(arguments, sql=script).stdout.strip()

    def prepare(self, env_file):
        # Existing resources are removable only after both custom labels match.
        self.remove_owned()
        self.owned = True
        image = self.command(['image', 'inspect', '--format', '{{.Id}}', self.image], check=False)
        if image.returncode:
            if shutil.disk_usage('/').free < 9 * 1024**3:
                raise RuntimeError('at least 9 GiB free space is required before pulling the HA fixture image')
            self.command(['pull', self.image])
            image = self.command(['image', 'inspect', '--format', '{{.Id}}', self.image])
        self.result['image_id'] = image.stdout.strip()
        self.command(['compose', '--env-file', str(env_file), '--project-name', self.project,
                      '--file', str(self.compose), 'up', '--detach'])
        container, volume = self.owned_resources()
        if not container or not volume:
            raise RuntimeError('Compose did not create the expected isolated fixture resources')
        self.container_id = container[0]
        while True:
            ready = self.command(['exec', self.container_id, 'pg_isready', '-h', '127.0.0.1',
                                  '-U', 'postgres', '-d', 'sigbot_extensions'], check=False)
            if ready.returncode == 0:
                break
            state = self.command(['container', 'inspect', '--format', '{{.State.Status}}', self.container_id]).stdout.strip()
            if state in {'exited', 'dead', 'stopped'}:
                self.command(['logs', '--tail', '100', self.container_id], check=False)
                raise RuntimeError(f'isolated extension PostgreSQL exited during initialization; inspect {self.log.name}')
            time.sleep(0.5)
        # Explicit owner bootstrap works after initialization and on an existing database too.
        script = self.bootstrap.read_text()
        self.sql(script)
        self.sql(script)
        evidence = self.sql("""SELECT jsonb_build_object('database',current_database(),
            'extensions',(SELECT jsonb_agg(jsonb_build_object('name',extname,'version',extversion) ORDER BY extname)
                FROM pg_extension WHERE extname IN ('timescaledb','vector')),
            'preload',current_setting('shared_preload_libraries'),
            'vector_distance',('[1,2,3]'::vector <-> '[4,6,3]'::vector),
            'bucket_equivalent',time_bucket(INTERVAL '7 days',TIMESTAMPTZ '2026-10-28 12:00:00+00',TIMESTAMPTZ '1970-01-01 00:00:00+00')
                =date_bin(INTERVAL '7 days',TIMESTAMPTZ '2026-10-28 12:00:00+00',TIMESTAMPTZ '1970-01-01 00:00:00+00'));""", scalar=True)
        actual = json.loads(evidence)
        if actual['database'] != 'sigbot_extensions' or {row['name'] for row in actual['extensions']} != {'timescaledb', 'vector'}:
            raise AssertionError('both extensions must be active in the isolated fixture database')
        if actual['vector_distance'] != 5 or not actual['bucket_equivalent']:
            raise AssertionError('actual extension functions failed their numeric contract')
        self.result.update(actual)
        self.result['container_id'] = self.container_id
        self.result['bootstrap_sha256'] = hashlib.sha256(script.encode()).hexdigest()
        migration = self.repository / 'migration/migrate.py'
        spec = importlib.util.spec_from_file_location('sigbot_extension_migrations', migration)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        rendered = module.MigrationRunner(migration.parent, 'postgres').postgres_script()
        self.sql(rendered)
        count = int(self.sql('SELECT count(*) FROM mig_version;', scalar=True))
        if count != len([path for path in migration.parent.iterdir()
                        if path.is_dir() and path.name.isdigit() and len(path.name) == 14]):
            raise AssertionError('isolated extension fixture did not apply every migration')
        self.result['schema_migrations'] = count
        self.result['migration_script_sha256'] = hashlib.sha256(rendered.encode()).hexdigest()

    def run(self):
        lock_path = Path(tempfile.gettempdir()) / 'sigbot-marketpulse-extensions.lock'
        with lock_path.open('a') as lock, tempfile.TemporaryDirectory(prefix='sigbot-extensions-') as private:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            env_file = Path(private) / 'fixture.env'
            env_file.write_text(f'MARKETPULSE_EXTENSIONS_PASSWORD={self.password}\n'
                                f'MARKETPULSE_POSTGRES_IMAGE={self.image}\n')
            env_file.chmod(0o600)
            try:
                self.prepare(env_file)
                self.result['rust'] = RustIntegration.execute(self.context, 'sigbot-datafeed', 'compute_extensions',
                    self.tests, {'MARKETPULSE_DATABASE_URL': self.url})
                self.result['status'] = 'PASS'
            except BaseException as error:
                self.result['status'] = 'FAIL'
                self.result['error'] = str(error).replace(self.password, '[REDACTED]').replace(self.url, '[REDACTED]')
                raise
            finally:
                try:
                    if self.owned:
                        self.remove_owned()
                    self.result['cleanup_complete'] = True
                except Exception as error:
                    self.result['status'] = 'FAIL'
                    self.result['cleanup_error'] = str(error)
                    raise
                finally:
                    self.context.evidence(f'extensions-{self.context.round_number}.json', json.dumps(self.result, indent=2))
        return self.result
