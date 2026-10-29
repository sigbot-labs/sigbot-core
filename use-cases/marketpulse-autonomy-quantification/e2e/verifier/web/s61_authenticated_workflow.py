import os
from pathlib import Path

from playwright.sync_api import expect, sync_playwright


def verify(context):
    state = os.environ.get("MARKETPULSE_BROWSER_STORAGE_STATE")
    if not state or not Path(state).is_file():
        raise RuntimeError("authenticated UI acceptance requires real Authguard browser storage state; no mocked login is permitted")
    directory = context.report_root / "web-full"
    directory.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True, executable_path=os.environ.get("MARKETPULSE_CHROMIUM_PATH"))
        session = browser.new_context(storage_state=state, viewport={"width": 1440, "height": 1000})
        page = session.new_page()
        page.set_default_timeout(min(60, context.timeout_seconds) * 1000)
        page.goto(context.endpoint("web") + "/#workflows", wait_until="networkidle")
        page.get_by_role("button", name="新建 Workflow", exact=True).click()
        page.get_by_label("Workflow 名称", exact=True).fill(f"Marketpulse {context.run_id}")
        for label in ("Datafeed", "Evaluator", "Strategy", "Order", "Wallet"):
            page.get_by_role("button", name="添加 " + label, exact=True).click()
        for source, target in ((1,2),(1,3),(2,3),(3,4),(4,5)):
            page.get_by_label("起点节点", exact=True).select_option(str(source))
            page.get_by_label("终点节点", exact=True).select_option(str(target))
            page.get_by_role("button", name="添加连线", exact=True).click()
        with page.expect_response(lambda response: response.request.method == "POST" and "/api/v1/workflows" in response.url) as saved:
            page.get_by_role("button", name="保存 Workflow", exact=True).click()
        assert saved.value.ok, f"real workflow save failed: HTTP {saved.value.status}"
        page.screenshot(path=str(directory / "saved-workflow.png"), full_page=True)
        with page.expect_response(lambda response: response.request.method == "POST" and "/start" in response.url) as started:
            page.get_by_role("button", name="运行 Workflow", exact=True).click()
        assert started.value.ok, f"real workflow start failed: HTTP {started.value.status}"
        for field in ("workflow-run-id", "trace-id", "span-id", "parent-span-id"):
            expect(page.get_by_test_id(field)).not_to_have_text("")
            expect(page.get_by_test_id(field)).not_to_have_text("—")
        page.screenshot(path=str(directory / "running-workflow-trace.png"), full_page=True)
        session.close()
        browser.close()
    return {"authenticated": True, "workflow_persisted_and_started": True, "screenshots": str(directory), "api_interception": False}
