# Sigbot workspace

The React workspace uses the real scoped API. It does not synthesize market prices, net asset value, execution counts, run IDs or trace evidence. Missing valuations remain empty; different assets are never summed as net value. The only references retained from the legacy UI are dashboard metric categories, workflow node categories and the account menu.

Run `npm ci && npm run dev` in this directory. Vite listens at `127.0.0.1:15173`; `VITE_API_TARGET` selects the real API upstream, and `VITE_AUTH_TARGET` selects the actual Authguard upstream. Both `/api` and `/auth` remain same-origin browser requests. No development login bypass or fabricated identity cookie is provided.

The sidebar contains Overview, market data, Workflow, sources and exchanges. Workflow nodes use the existing graph schema, support canvas handles and accessible connection controls, and preserve optional v2 topic kinds. Node configuration uses `config.module` plus the existing service `argument_json`; environment-variable references supply host credentials. Saving a draft does not deploy it. Starting creates a durable run, and events display actual persisted run/trace/span values. A successful asynchronous start response is not a trade fill.

API routes are `/api/v1/identity`, `/overview`, `/market`, `/connections`, `/workflows`, `/notifications` and `/notifications/read`; scoped requests carry only `env`. Tenant and recipient identity come from verified Authguard context. Workflow save never accepts a client-selected tenant. Notification read receipts use the delivery identity `(run_id, producer_node_id, seq, channel, recipient)` within the signed scope.

Build with `npm run build`. Browser tests use `npm test`, with `PLAYWRIGHT_CHROMIUM_EXECUTABLE` optionally pointing at an installed Chromium. `SIGBOT_WEB_URL` selects a real deployment and `SIGBOT_PLAYWRIGHT_STORAGE_STATE` can load a state obtained through actual Authguard login. Tests never intercept API routes. Local navigation, account panels, graph editing and screenshot checks can run without connected services; authenticated save/run/event acceptance requires the real gateway origin and protected API. The use-case verifier separately performs the complete functional browser gate.

The container uses unprivileged nginx. Gateway routes send the public shell to web and protected `/api` requests to Sigbot; direct nginx `/api` and `/auth` access returns 404. Helm needs the operator's real public hostname and Authguard application binding before a hosted-login browser acceptance run.
