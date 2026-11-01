#!/usr/bin/env node

export class PrStatusReporter {
  static marker = "<!-- sigbot-pipeline-status -->";

  static render(context) {
    const icons = { success: "✅", failure: "❌", cancelled: "❌", skipped: "⏭️", pending: "⏳" };
    const lines = [`### ${context.kind === "release" ? "Release" : "PR build and test"}`, "",
      `Commit: \`${context.sha}\` · [Actions and evidence](${context.url})`, "",
      "| Step | Status |", "| --- | --- |"];
    for (const [name, item] of Object.entries(context.results)) {
      const status = item.result ?? item.outcome ?? item.status ?? "pending";
      lines.push(`| ${name.replaceAll("|", "\\|")} | ${icons[status] ?? "⏳"} ${status} |`);
    }
    if (context.version) lines.push("", `Artifact version: \`${context.version}\``);
    lines.push("", `Phase: ${context.phase}`);
    return lines.join("\n");
  }

  static merge(previous, context) {
    const match = previous?.match(/<!-- sigbot-state:([A-Za-z0-9+/=]+) -->/);
    const state = match ? JSON.parse(Buffer.from(match[1], "base64").toString()) : {};
    const old = state[context.kind];
    if (old && BigInt(old.run) > BigInt(context.run)) return previous;
    state[context.kind] = { run: context.run, body: this.render(context) };
    return [this.marker, ...["ci", "release"].filter((key) => state[key]).map((key) => state[key].body),
      `<!-- sigbot-state:${Buffer.from(JSON.stringify(state)).toString("base64")} -->`].join("\n\n");
  }

  static async main() {
    const context = {
      kind: process.env.SIGBOT_COMMENT_KIND ?? "ci", phase: process.env.SIGBOT_COMMENT_PHASE ?? "completed",
      sha: process.env.SIGBOT_HEAD_SHA ?? "unknown", run: process.env.GITHUB_RUN_ID ?? "0",
      url: process.env.SIGBOT_RUN_URL ?? "", version: process.env.SIGBOT_ARTIFACT_VERSION,
      results: JSON.parse(process.env.SIGBOT_RESULTS ?? "{}"),
    };
    if (process.argv.includes("--dry-run")) {
      console.log(this.merge("", context));
      return;
    }
    const repository = process.env.GITHUB_REPOSITORY;
    const number = process.env.SIGBOT_PR_NUMBER;
    const token = process.env.GH_TOKEN;
    if (!/^[\w.-]+\/[\w.-]+$/.test(repository ?? "") || !/^\d+$/.test(number ?? "") || !token) {
      throw new Error("Missing repository, PR number or comment credentials");
    }
    const base = `${process.env.GITHUB_API_URL ?? "https://api.github.com"}/repos/${repository}/issues/${number}/comments`;
    const request = async (url, method = "GET", body) => {
      const response = await fetch(url, { method,
        headers: { Accept: "application/vnd.github+json", Authorization: `Bearer ${token}`,
          "X-GitHub-Api-Version": "2022-11-28", "Content-Type": "application/json" },
        body: body ? JSON.stringify(body) : undefined });
      if (!response.ok) throw new Error(`PR comment API ${method} failed: HTTP ${response.status}`);
      return response.status === 204 ? null : response.json();
    };
    if (context.phase === "completed") {
      const jobs = await request(`${process.env.GITHUB_API_URL ?? "https://api.github.com"}/repos/${repository}/actions/runs/${context.run}/jobs?per_page=100`);
      for (const job of jobs.jobs ?? []) {
        if (/^Report|^report/i.test(job.name)) continue;
        for (const step of job.steps ?? []) {
          context.results[`${job.name} / ${step.name}`] = { result: step.conclusion ?? step.status };
        }
      }
    }
    let existing;
    for (let page = 1; ; page++) {
      const comments = await request(`${base}?per_page=100&page=${page}`);
      existing = comments.find((item) => item.user?.login === "github-actions[bot]" && item.body?.includes(this.marker));
      if (existing || comments.length < 100) break;
    }
    const body = this.merge(existing?.body, context);
    if (body === existing?.body) return;
    await request(existing ? `${process.env.GITHUB_API_URL ?? "https://api.github.com"}/repos/${repository}/issues/comments/${existing.id}` : base,
      existing ? "PATCH" : "POST", { body });
  }
}

if (process.argv[1]?.endsWith("upsert-pr-status-comment.mjs")) await PrStatusReporter.main();
