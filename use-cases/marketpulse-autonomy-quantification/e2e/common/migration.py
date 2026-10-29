"""Real additive upgrade in an isolated container database; no credentials or case DB."""
import datetime
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import tempfile
import time
import uuid


class LegacyMigrationProbe:
    def __init__(self, context):
        self.context = context
        self.root = context.root.parents[2]
        self.directory = self.root / 'tooling/deploy/migrations/postgresql'
        self.database = 'sigbot_legacy_probe_' + uuid.uuid4().hex
        self.container = os.environ.get('MARKETPULSE_MIGRATION_CONTAINER', 'sigbot-migration-check')
        if not re.fullmatch(r'sigbot-migration-check(?:-[a-z0-9_-]+)?', self.container):
            raise ValueError('migration container must use the dedicated sigbot-migration-check name or prefix')
        self.command = shlex.split(os.environ.get('MARKETPULSE_MIGRATION_COMMAND', 'sudo -n podman'))
        if self.command not in [['podman'], ['docker'], ['sudo', 'podman'], ['sudo', 'docker'],
                                ['sudo', '-n', 'podman'], ['sudo', '-n', 'docker']]:
            raise ValueError('migration command must select local Docker/Podman without remote arguments')
        self.command += ['--remote=false'] if self.command[-1] == 'podman' else ['--host', 'unix:///var/run/docker.sock']
        self.container_id = None
        self.deadline = time.monotonic() + context.timeout_seconds
        self.created = False
        self.report = context.evidence(f'upgrade-{context.round_number}.json', '{}\n')
        self.log = context.evidence(f'postgres-{context.round_number}.log', '')
        specification = importlib.util.spec_from_file_location('sigbot_migrations', self.directory.parent / 'migrate.py')
        module = importlib.util.module_from_spec(specification)
        specification.loader.exec_module(module)
        self.runner = module.MigrationRunner
        self.result = {'database': self.database, 'container': self.container,
                       'timestamp': datetime.datetime.now(datetime.timezone.utc).isoformat(),
                       'boundary': 'actual PostgreSQL; synthetic legitimate legacy records; no case database or credentials',
                       'checks': {}, 'migration_sha256': {}, 'temporary_database_removed': False, 'status': 'RUNNING'}

    def execute(self, argv, *, sql=None, cleanup=False):
        remaining = 20 if cleanup else self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError('legacy migration scenario exceeded its deadline')
        return subprocess.run(self.command + argv, input=sql, text=True, capture_output=True,
                              timeout=remaining, check=False,
                              env={key: value for key, value in os.environ.items()
                                   if key not in {'DOCKER_HOST', 'DOCKER_CONTEXT', 'CONTAINER_HOST', 'CONTAINER_CONNECTION'}})

    def sql(self, sql, *, database=None, scalar=False, cleanup=False):
        selected = database or self.database
        if selected not in ('postgres', self.database):
            raise ValueError('SQL target differs from the owned temporary database')
        command = ['exec', '-i', self.container_id or self.container, 'psql', '--no-psqlrc', '--no-password',
                   '--host', '/var/run/postgresql', '--port', '5432', '--username', 'postgres',
                   '--dbname', selected, '--set', 'ON_ERROR_STOP=on', '--quiet']
        if scalar:
            command += ['--tuples-only', '--no-align']
        result = self.execute(command, sql=sql, cleanup=cleanup)
        with self.log.open('a') as stream:
            stream.write(result.stdout)
            stream.write(result.stderr)
        if result.returncode:
            raise RuntimeError(f'isolated migration SQL failed; inspect {self.log.name}: {result.stderr.strip()}')
        return result.stdout.strip()

    def validate_isolation(self):
        result = self.execute(['container', 'inspect', '--format',
                               '{{.Id}}|{{json .Name}}|{{json .State.Running}}', self.container])
        if result.returncode:
            raise RuntimeError('dedicated migration container does not exist or cannot be inspected')
        identifier, raw_name, raw_running = result.stdout.strip().split('|')
        name, running = json.loads(raw_name), json.loads(raw_running)
        if not re.fullmatch(r'[a-f0-9]{64}', identifier):
            raise RuntimeError('container runtime returned an invalid immutable container ID')
        if name.lstrip('/') != self.container or running is not True:
            raise RuntimeError('migration container identity or running state differs from its configuration')
        self.container_id = identifier
        names = self.sql('SELECT datname FROM pg_database WHERE NOT datistemplate ORDER BY datname;',
                         database='postgres', scalar=True).splitlines()
        if any(not re.fullmatch(r'postgres|sigbot_check_[a-z0-9_]+|sigbot_legacy_probe_[a-f0-9]{16,32}', name)
               for name in names):
            raise RuntimeError('migration container contains a database outside its dedicated test namespace')
        if self.database in names:
            raise RuntimeError('random temporary database already exists; refusing to adopt it')
        version = self.sql("SELECT current_setting('server_version_num');", database='postgres', scalar=True)
        if int(version) < 160000:
            raise RuntimeError('legacy migration acceptance requires PostgreSQL 16 or newer')
        self.result['isolation'] = {'container_id': identifier, 'postgres_version_num': int(version),
                                    'local_unix_socket': True, 'dedicated_database_namespace': True,
                                    'temporary_database_absent_before_creation': True}

    def cleanup(self):
        if self.created:
            # The random name was absent before this invocation; never delete another database.
            self.sql(f'DROP DATABASE IF EXISTS {self.database} WITH (FORCE);', database='postgres', cleanup=True)
            remaining = self.sql(f"SELECT count(*) FROM pg_database WHERE datname='{self.database}';",
                                 database='postgres', scalar=True, cleanup=True)
            if remaining != '0':
                raise RuntimeError('owned temporary database still exists after cleanup')
        self.result['temporary_database_removed'] = True

    @staticmethod
    def financial_query(table):
        columns = {
            's_wallets': 'id,tenant_id,exchange,mode,account_type,created_at,updated_at,del_flag',
            's_ledger': 'wallet_id,order_id,trade_id,symbol,side,price,qty,fee,fee_asset,exchange_order_id,exchange_trade_id,ts',
            's_balances': 'wallet_id,asset,available,locked,updated_at',
            's_positions': 'wallet_id,symbol,side,size,entry_price,realized_pnl,updated_at',
            'equity_snapshots': 'wallet_id,equity,ts',
            't_workflow': 'id,name,flow_json,created_at,updated_at,del_flag',
            't_workflow_run': 'id,workflow_id,params,start_date,end_date,run_type,state,profit,node_states',
        }[table]
        order = {'s_wallets': 'id', 's_ledger': 'wallet_id,order_id,trade_id',
                 's_balances': 'wallet_id,asset', 's_positions': 'wallet_id,symbol,side',
                 'equity_snapshots': 'wallet_id,ts', 't_workflow': 'id', 't_workflow_run': 'id'}[table]
        return f'SELECT COALESCE(jsonb_agg(to_jsonb(record)),\'[]\'::jsonb) FROM (SELECT {columns} FROM {table} ORDER BY {order}) record'

    def run(self):
        try:
            self.validate_isolation()
            # Mark ownership before CREATE so a timed-out server response is also cleaned up.
            self.created = True
            self.sql(f'CREATE DATABASE {self.database};', database='postgres')
            if self.sql('SELECT current_database();', scalar=True) != self.database:
                raise RuntimeError('migration connection did not enter the owned temporary database')
            files = sorted(self.directory.glob('*/*.ddl.sql'), key=self.runner.migration_key)
            baseline = [file for file in files if file.parent.name[:4] in ('2024', '2025')]
            if len(baseline) != 4 or len(files) < 21:
                raise AssertionError('unexpected legacy baseline or incomplete current migration set')
            with tempfile.TemporaryDirectory(prefix='sigbot-legacy-baseline-') as temporary:
                root = Path(temporary)
                for file in baseline:
                    target = root / file.relative_to(self.directory)
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(file, target)
                self.sql(self.runner(root).script())
            self.result['baseline_migrations'] = len(baseline)
            fixture = self.fixture()
            self.result['fixture_sha256'] = hashlib.sha256(fixture.encode()).hexdigest()
            self.result['runner_sha256'] = hashlib.sha256((self.directory.parent / 'migrate.py').read_bytes()).hexdigest()
            self.sql(fixture)
            tables = ['s_wallets', 's_ledger', 's_balances', 's_positions', 'equity_snapshots', 't_workflow', 't_workflow_run']
            self.sql('CREATE TABLE legacy_acceptance_snapshot(name TEXT PRIMARY KEY,payload JSONB NOT NULL);')
            for table in tables:
                self.sql(f"INSERT INTO legacy_acceptance_snapshot SELECT '{table}',({self.financial_query(table)});")
            self.result['legacy_counts'] = json.loads(self.sql('SELECT jsonb_object_agg(name,jsonb_array_length(payload)) FROM legacy_acceptance_snapshot;', scalar=True))
            self.result['migration_sha256'] = {str(file.relative_to(self.directory)): hashlib.sha256(file.read_bytes()).hexdigest() for file in files}
            full_script = self.runner(self.directory).script()
            self.result['rendered_script_sha256'] = hashlib.sha256(full_script.encode()).hexdigest()
            self.context.evidence(f'rendered-{self.context.round_number}.sql', full_script)
            self.sql(full_script)
            for table in tables:
                preserved = self.sql(f"SELECT payload=({self.financial_query(table)}) FROM legacy_acceptance_snapshot WHERE name='{table}';", scalar=True)
                assert preserved == 't', f'legacy table data changed: {table}'
                self.result['checks'][f'{table}_legacy_columns_preserved'] = True
            self.check('uuid_workflows_mapped_once', '''SELECT COUNT(*)=(SELECT COUNT(*) FROM t_workflow)
                AND COUNT(DISTINCT m.workflow_id)=COUNT(*)
                AND bool_and(w.name=t.name AND w.flow_json=t.flow_json AND w.created_at=t.created_at
                  AND w.updated_at=t.updated_at AND w.del_flag=CASE WHEN t.del_flag THEN 1 ELSE 0 END
                  AND w.env='legacy' AND w.tenant_id IS NULL AND w.space_id IS NULL AND w.status='STOPPED')
                FROM t_workflow t JOIN workflow_legacy_id m ON m.legacy_id=t.id JOIN s_workflow w ON w.id=m.workflow_id''')
            self.check('uuid_runs_bind_mapped_workflows', '''SELECT COUNT(*)=(SELECT COUNT(*) FROM t_workflow_run)
                AND bool_and(r.run_id=t.id::TEXT AND r.workflow_id=m.workflow_id AND r.env='legacy'
                  AND r.tenant_id IS NULL AND r.space_id IS NULL AND r.params=t.params AND r.state=t.state
                  AND r.started_at=t.start_date AND r.ended_at IS NOT DISTINCT FROM t.end_date)
                FROM t_workflow_run t JOIN workflow_legacy_id m ON m.legacy_id=t.workflow_id
                JOIN workflow_run r ON r.run_id=t.id::TEXT''')
            self.check('legacy_wallet_scope_and_account_backfill', '''SELECT COUNT(*)=2 AND bool_and(env='legacy'
                AND space_id IS NULL AND account_id=id AND portfolio_version=0) FROM s_wallets''')
            self.check('no_fabricated_double_entry_journal', 'SELECT COUNT(*)=0 FROM wallet_journal')
            self.check('precision_preserves_22_integral_digits', '''SELECT available=1234567890123456789012.12345678::numeric
                FROM s_balances WHERE wallet_id=7000000001 AND asset='USDT' ''')
            self.check('retained_wallet_foreign_keys', '''SELECT COUNT(*)=4 FROM pg_constraint WHERE contype='f'
                AND confrelid='s_wallets'::regclass AND conrelid IN ('s_ledger'::regclass,'s_balances'::regclass,
                's_positions'::regclass,'equity_snapshots'::regclass)''')
            self.sql('''DO $$ BEGIN
                BEGIN UPDATE s_ledger SET fee=fee+1 WHERE wallet_id=7000000001;
                    RAISE EXCEPTION 'legacy ledger mutation unexpectedly accepted';
                EXCEPTION WHEN others THEN
                    IF SQLERRM NOT LIKE 'immutable wallet facts:%' THEN RAISE; END IF;
                END;
            END $$;''')
            self.result['checks']['retained_legacy_ledger_immutable_after_upgrade'] = True
            count = int(self.sql('SELECT count(*) FROM schema_migration;', scalar=True))
            assert count == len(files)
            before = self.sql('SELECT jsonb_build_object(\'legacy\',(SELECT count(*) FROM t_workflow),\'mapped\',(SELECT count(*) FROM workflow_legacy_id),\'new\',(SELECT count(*) FROM s_workflow),\'runs\',(SELECT count(*) FROM workflow_run));', scalar=True)
            self.sql(full_script)
            after = self.sql('SELECT jsonb_build_object(\'legacy\',(SELECT count(*) FROM t_workflow),\'mapped\',(SELECT count(*) FROM workflow_legacy_id),\'new\',(SELECT count(*) FROM s_workflow),\'runs\',(SELECT count(*) FROM workflow_run));', scalar=True)
            assert before == after and int(self.sql('SELECT count(*) FROM schema_migration;', scalar=True)) == count
            self.result['checks']['full_runner_second_run_is_idempotent'] = True
            self.result['schema_migrations'] = count
            self.result['status'] = 'PASS'
        except BaseException as error:
            self.result['status'] = 'FAIL'
            self.result['error'] = str(error)
            raise
        finally:
            try:
                self.cleanup()
            except Exception as cleanup_error:
                self.result['status'] = 'FAIL'
                self.result['cleanup_error'] = str(cleanup_error)
                raise
            finally:
                self.report.write_text(json.dumps(self.result, ensure_ascii=False, indent=2) + '\n')
        return self.result

    def check(self, name, query):
        assert self.sql(query + ';', scalar=True) == 't', name
        self.result['checks'][name] = True

    def fixture(self):
        return (self.context.root / 'config/sigbot/legacy_migration.sql').read_text()
