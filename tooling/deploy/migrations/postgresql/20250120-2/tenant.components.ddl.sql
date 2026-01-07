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

-- Add encryption_public_key column (Base64 encoded RSA public key)
ALTER TABLE sys_tenant 
ADD COLUMN IF NOT EXISTS encryption_public_key TEXT NULL;

-- Add encryption_private_key_hash column (SHA256 hash of private key for verification)
ALTER TABLE sys_tenant 
ADD COLUMN IF NOT EXISTS encryption_private_key_hash VARCHAR(64) NULL;

-- Add cloud_managed_key column (boolean flag indicating if platform manages the private key)
ALTER TABLE sys_tenant 
ADD COLUMN IF NOT EXISTS cloud_managed_key BOOLEAN NULL DEFAULT FALSE;

-- Add encrypted_private_key column (encrypted with system master key, only stored if cloud_managed_key is true)
ALTER TABLE sys_tenant 
ADD COLUMN IF NOT EXISTS encrypted_private_key TEXT NULL;

-- Add components column (JSONB for PostgreSQL, TEXT for SQLite)
-- Stores component deployment configurations with encrypted passwords
ALTER TABLE sys_tenant 
ADD COLUMN IF NOT EXISTS components JSONB NULL;

-- Create index on encryption_public_key for faster lookups
CREATE INDEX IF NOT EXISTS idx_tenant_encryption_public_key ON sys_tenant(encryption_public_key) WHERE encryption_public_key IS NOT NULL;

-- Create index on cloud_managed_key for faster lookups
CREATE INDEX IF NOT EXISTS idx_tenant_cloud_managed_key ON sys_tenant(cloud_managed_key) WHERE cloud_managed_key IS TRUE;

-- Comments
COMMENT ON COLUMN sys_tenant.encryption_public_key IS 'Base64 encoded RSA public key for encrypting component passwords. Each tenant has its own key pair.';
COMMENT ON COLUMN sys_tenant.encryption_private_key_hash IS 'SHA256 hash of the private key for verification. Private key should be shown only once during tenant creation.';
COMMENT ON COLUMN sys_tenant.encrypted_private_key IS 'Base64 encoded, encrypted tenant private key (encrypted with system master key using AES-256-GCM). Only stored if cloud_managed_key is true.';
COMMENT ON COLUMN sys_tenant.cloud_managed_key IS 'If true, platform stores encrypted private key for recovery. This prevents data loss if user loses their private key.';
COMMENT ON COLUMN sys_tenant.components IS 'JSON object containing deployed component configurations (instances, connection info, encrypted passwords, performance params).';

