import os
from urllib.parse import parse_qs, urlparse

from playwright.sync_api import sync_playwright


def verify(context):
    directory = context.report_root / "web"
    directory.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True, executable_path=os.environ.get("MARKETPULSE_CHROMIUM_PATH"))
        page = browser.new_page(viewport={"width": 1440, "height": 1000}, device_scale_factor=1)
        page.set_default_timeout(min(context.timeout_seconds, 30) * 1000)
        errors = []
        page.on("pageerror", lambda error: errors.append(str(error)))
        page.goto(context.endpoint("web"), wait_until="networkidle")
        screenshots = []
        for index, label in enumerate(("Overview", "市场行情", "Workflow", "数据源", "交易所")):
            page.get_by_role("link", name=label, exact=True).click()
            page.wait_for_load_state("networkidle")
            filename = directory / f"{index:02d}-{context.round_number}.png"
            page.screenshot(path=str(filename), full_page=True)
            screenshots.append(str(filename))
            assert page.locator("main").inner_text().strip(), f"empty route: {label}"
            if label == "市场行情":
                page.get_by_role("button", name="新闻与政策", exact=True).click()
                for source_label, source_id in (("X", "twitter"), ("美联储", "fed"), ("Treasury", "treasury")):
                    with page.expect_request(lambda request: urlparse(request.url).path == "/api/v1/market"
                        and parse_qs(urlparse(request.url).query).get("source") == [source_id]) as request:
                        page.get_by_role("button", name=source_label, exact=True).click()
                    assert parse_qs(urlparse(request.value.url).query).get("category") == ["news"]
                filename = directory / f"policy-sources-{context.round_number}.png"
                page.screenshot(path=str(filename), full_page=True)
                screenshots.append(str(filename))
        page.get_by_role("button", name="通知", exact=True).click()
        filename = directory / f"notifications-{context.round_number}.png"
        page.screenshot(path=str(filename), full_page=True)
        screenshots.append(str(filename))
        page.keyboard.press("Escape")
        page.get_by_role("button", name="个人菜单", exact=True).click()
        filename = directory / f"profile-{context.round_number}.png"
        page.screenshot(path=str(filename), full_page=True)
        screenshots.append(str(filename))
        page.keyboard.press("Escape")
        page.get_by_role("link", name="Workflow", exact=True).click()
        page.get_by_role("button", name="新建 Workflow", exact=True).click()
        page.get_by_label("Workflow 名称", exact=True).fill("Marketpulse screenshot verification")
        for label in ("Datafeed", "Evaluator", "Strategy"):
            page.get_by_role("button", name="添加 " + label, exact=True).click()
        assert page.locator(".react-flow__node").count() == 3
        page.get_by_label("起点节点", exact=True).select_option("1")
        page.get_by_label("终点节点", exact=True).select_option("2")
        page.get_by_role("button", name="添加连线", exact=True).click()
        assert page.get_by_test_id("workflow-edge-count").inner_text() == "1 条连线"
        page.wait_for_function("""() => {
            const canvas = document.querySelector('.react-flow').getBoundingClientRect();
            return [...document.querySelectorAll('.react-flow__node')].every(node => {
                const box = node.getBoundingClientRect();
                return box.left >= canvas.left && box.right <= canvas.right &&
                       box.top >= canvas.top && box.bottom <= canvas.bottom;
            });
        }""")
        filename = directory / f"workflow-editor-{context.round_number}.png"
        page.screenshot(path=str(filename), full_page=True)
        screenshots.append(str(filename))
        assert not errors, errors
        browser.close()
    return {"screenshots": screenshots, "browser": "real Chromium via Playwright", "page_errors": errors,
            "source_query_ids": ["twitter", "fed", "treasury"],
            "scope": "navigation, notices, account menu and real graph editing; authenticated persistence is a separate gate"}
