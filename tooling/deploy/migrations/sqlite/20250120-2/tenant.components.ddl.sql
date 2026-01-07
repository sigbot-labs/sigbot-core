-- SPDX-License-Identifier: GNU GENERAL PUBLIC LICENSE Version 3
--
-- Copyleft (c) 2024 James Wong. This file is part of James Wong.
-- is free software: you can redistribute it and/or modify it under
-- the terms of the GNU General Public License as published by the
-- Free Software Foundation, either version 3 of the License, or
-- (at your option) any later version.
--
-- James Wong is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
-- GNU General Public License for more details.
--
-- You should have received a copy of the GNU General Public License
-- along with James Wong.  If not, see <https://www.gnu.org/licenses/>.
--
-- IMPORTANT: Any software that fully or partially contains or uses materials
-- covered by this license must also be released under the GNU GPL license.
-- This includes modifications and derived works.

-- Add encryption and components fields to sys_tenant table
-- Note: This migration assumes sys_tenant table already exists

-- SQLite doesn't support ALTER TABLE ADD COLUMN IF NOT EXISTS directly
-- We need to check if columns exist first (SQLite 3.32.0+ supports this)
-- For older SQLite versions, we'll use a different approach

-- Add encryption_public_key column
-- Note: SQLite will ignore this if column already exists (in newer versions)
ALTER TABLE sys_tenant 
ADD COLUMN encryption_public_key TEXT NULL;

-- Add encryption_private_key_hash column
ALTER TABLE sys_tenant 
ADD COLUMN encryption_private_key_hash VARCHAR(64) NULL;

-- Add cloud_managed_key column (boolean flag indicating if platform manages the private key)
ALTER TABLE sys_tenant 
ADD COLUMN cloud_managed_key BOOLEAN NULL DEFAULT 0;

-- Add encrypted_private_key column (encrypted with system master key, only stored if cloud_managed_key is true)
ALTER TABLE sys_tenant 
ADD COLUMN encrypted_private_key TEXT NULL;

-- Add components column (TEXT storing JSON for SQLite)
ALTER TABLE sys_tenant 
ADD COLUMN components TEXT NULL;

-- Create index on encryption_public_key
CREATE INDEX IF NOT EXISTS idx_tenant_encryption_public_key ON sys_tenant(encryption_public_key);

-- Create index on cloud_managed_key
CREATE INDEX IF NOT EXISTS idx_tenant_cloud_managed_key ON sys_tenant(cloud_managed_key);

