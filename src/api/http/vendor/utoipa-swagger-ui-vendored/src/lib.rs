// SPDX-License-Identifier: MIT OR Apache-2.0

/// Swagger UI archive consumed by `utoipa-swagger-ui` during its build.
pub static SWAGGER_UI_VENDORED: &[u8] =
    include_bytes!("../../../src/swagger/swagger-ui-5.17.14.zip");
