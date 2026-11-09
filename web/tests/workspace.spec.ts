import { test, expect } from "@playwright/test";

// These checks use the actual web server. API responses are never intercepted.
test("workspace navigation and account panels remain accessible", async ({
  page,
}, testInfo) => {
  await page.goto("/");
  for (const label of [
    "Overview",
    "市场行情",
    "Workflow",
    "数据源",
    "交易所",
  ]) {
    await page.getByRole("link", { name: label, exact: true }).click();
    await expect(page.getByRole("main")).toBeVisible();
    await expect(
      page.getByRole("heading", { name: label, exact: true }),
    ).toBeVisible();
    await page.screenshot({
      path: testInfo.outputPath(`${label}.png`),
      fullPage: true,
    });
  }
  await page.getByRole("button", { name: "通知", exact: true }).click();
  await expect(page.getByRole("dialog", { name: "通知中心" })).toBeVisible();
  await page.keyboard.press("Escape");
  await page.getByRole("button", { name: "个人菜单" }).click();
  await expect(page.getByRole("menu", { name: "账户菜单" })).toBeVisible();
});

test("workflow graph edits use real canvas nodes and edges", async ({
  page,
}, testInfo) => {
  await page.goto("/#workflows");
  await page.getByRole("button", { name: "新建 Workflow" }).click();
  await page
    .getByLabel("Workflow 名称", { exact: true })
    .fill("Playwright graph verification");
  await page
    .getByRole("button", { name: "添加 Datafeed", exact: true })
    .click();
  await page
    .getByRole("button", { name: "添加 Evaluator", exact: true })
    .click();
  await page
    .getByRole("button", { name: "添加 Strategy", exact: true })
    .click();
  await expect(page.locator(".react-flow__node")).toHaveCount(3);
  await expect
    .poll(async () => {
      const canvas = await page.getByTestId("workflow-canvas").boundingBox();
      const positions = await page
        .locator(".react-flow__node")
        .evaluateAll((elements) =>
          elements.map((element) => {
            const box = element.getBoundingClientRect();
            return {
              left: box.left,
              right: box.right,
              top: box.top,
              bottom: box.bottom,
            };
          }),
        );
      return (
        canvas !== null &&
        positions.every(
          (box) =>
            box.left >= canvas.x &&
            box.right <= canvas.x + canvas.width &&
            box.top >= canvas.y &&
            box.bottom <= canvas.y + canvas.height,
        )
      );
    })
    .toBe(true);
  await page.getByLabel("起点节点", { exact: true }).selectOption("1");
  await page.getByLabel("终点节点", { exact: true }).selectOption("2");
  await page.getByRole("button", { name: "添加连线", exact: true }).click();
  await expect(page.getByTestId("workflow-edge-count")).toHaveText("1 条连线");
  await expect(page.locator(".react-flow__edge")).toHaveCount(1);
  await expect(
    page.getByRole("button", { name: "运行 Workflow", exact: true }),
  ).toBeDisabled();
  await page.screenshot({
    path: testInfo.outputPath("workflow-editor.png"),
    fullPage: true,
  });
});
